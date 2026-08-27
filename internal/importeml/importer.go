// Package importeml provides .eml and .eml.gz file import into msgvault.
//
// It walks a directory tree (or explicit file list) for .eml and .eml.gz
// files as produced by GAM, GYB, and gmvault backups, then feeds each raw
// message through the shared importer ingest path
// (importer.IngestRawMessage), reusing the same store, dedup, checkpoint,
// and attachment handling as the mbox and emlx importers.
package importeml

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"time"

	"go.kenn.io/msgvault/internal/importer"
	"go.kenn.io/msgvault/internal/store"
)

// sourceTypeEml is the default sources.source_type for .eml imports.
const sourceTypeEml = "eml"

const defaultMaxEmlMessageBytes int64 = 128 << 20 // 128 MiB

// EmlImportOptions configures a .eml / .eml.gz import.
type EmlImportOptions struct {
	// SourceType is the sources.source_type value (defaults to "eml").
	SourceType string

	// Identifier is the sources.identifier (e.g. "you@example.com").
	// Required.
	Identifier string

	// Recursive scans directories recursively for .eml/.eml.gz files.
	Recursive bool

	// Labels, if non-empty, are applied to all imported messages.
	Labels []string

	// NoResume forces a fresh import even if an active sync run exists.
	NoResume bool

	// CheckpointInterval controls how often (in messages) to persist
	// progress. Defaults to 200.
	CheckpointInterval int

	// AttachmentsDir controls where attachments are written.
	// Empty means no disk storage (messages are still imported).
	AttachmentsDir string

	// MaxMessageBytes limits the maximum uncompressed size of a single
	// .eml file to read. Defaults to 128 MiB.
	MaxMessageBytes int64

	// IngestFunc overrides message ingestion (for tests). If nil, the
	// default importer.IngestRawMessage is used.
	IngestFunc func(
		ctx context.Context, st *store.Store,
		sourceID int64, identifier, attachmentsDir string,
		labelIDs []int64, sourceMsgID, rawHash string,
		raw []byte, fallbackDate time.Time,
		log *slog.Logger,
	) error

	// Logger is optional; defaults to slog.Default().
	Logger *slog.Logger
}

// EmlImportSummary reports the results of a .eml import.
type EmlImportSummary struct {
	SourceID   int64
	WasResumed bool
	Duration   time.Duration

	FilesFound     int64
	BytesProcessed int64

	MessagesProcessed int64
	MessagesAdded     int64
	MessagesUpdated   int64
	MessagesSkipped   int64
	LabelsUpdated     int64
	Errors            int64
	HardErrors        bool
}

// emlCheckpoint is persisted in the sync run's cursor to support resume.
type emlCheckpoint struct {
	// Roots is the (sorted) set of import roots this run covers, used to
	// detect that a resume request targets the same inputs.
	Roots []string `json:"roots"`
	// LastFile is the last fully-processed discovered file path. Files
	// lexicographically <= LastFile are skipped on resume.
	LastFile string `json:"last_file"`
}

// ImportEmlPaths imports .eml and .eml.gz files discovered under the given
// paths into the msgvault database.
//
// Messages are deduplicated by content hash (sha256 of raw MIME) via
// source_message_id, so re-importing the same files is safe. When the same
// content appears more than once, later occurrences merge their labels onto
// the first.
func ImportEmlPaths(
	ctx context.Context, st *store.Store,
	paths []string, opts EmlImportOptions,
) (*EmlImportSummary, error) {
	if opts.SourceType == "" {
		opts.SourceType = sourceTypeEml
	}
	if opts.Identifier == "" {
		return nil, errors.New("identifier is required")
	}
	if opts.CheckpointInterval <= 0 {
		opts.CheckpointInterval = 200
	}
	if opts.MaxMessageBytes <= 0 {
		opts.MaxMessageBytes = defaultMaxEmlMessageBytes
	}
	ingestFn := opts.IngestFunc
	if ingestFn == nil {
		ingestFn = importer.IngestRawMessage
	}
	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}

	start := time.Now()
	summary := &EmlImportSummary{}

	// Discover files up front so the checkpoint can skip already-processed
	// entries deterministically.
	files, err := discoverFiles(paths, opts.Recursive)
	if err != nil {
		return nil, fmt.Errorf("discover files: %w", err)
	}
	sort.Strings(files)
	summary.FilesFound = int64(len(files))
	if len(files) == 0 {
		summary.Duration = time.Since(start)
		return summary, nil
	}

	roots := normalizedRoots(paths)

	src, err := st.GetOrCreateSource(opts.SourceType, opts.Identifier)
	if err != nil {
		return nil, fmt.Errorf("get/create source: %w", err)
	}
	summary.SourceID = src.ID

	// Resume support.
	var (
		syncID     int64
		cp         store.Checkpoint
		startAfter string
	)

	if !opts.NoResume {
		active, err := st.GetActiveSync(src.ID)
		if err != nil && !errors.Is(err, store.ErrSyncRunNotFound) {
			return nil, fmt.Errorf("check active sync: %w", err)
		}
		if active != nil {
			cp.MessagesProcessed = active.MessagesProcessed
			cp.MessagesAdded = active.MessagesAdded
			cp.MessagesUpdated = active.MessagesUpdated
			cp.ErrorsCount = active.ErrorsCount
			if active.CursorBefore.Valid && active.CursorBefore.String != "" {
				var ecp emlCheckpoint
				if err := json.Unmarshal(
					[]byte(active.CursorBefore.String), &ecp,
				); err == nil {
					if !slices.Equal(ecp.Roots, roots) {
						return nil, fmt.Errorf(
							"active eml import is for different paths (%v), not %v; rerun with --no-resume to start fresh",
							ecp.Roots, roots,
						)
					}
					syncID = active.ID
					startAfter = ecp.LastFile
					summary.WasResumed = true
					log.Info("resuming eml import",
						"roots", roots,
						"last_file", startAfter,
						"processed", cp.MessagesProcessed,
					)
				}
			}
		}
	}

	if syncID == 0 {
		syncID, err = st.StartSync(src.ID, "import-eml")
		if err != nil {
			return nil, fmt.Errorf("start sync: %w", err)
		}
	}

	failSync := func(msg string) {
		if fsErr := st.FailSync(syncID, msg); fsErr != nil {
			log.Warn("failed to record sync failure", "error", fsErr)
		}
	}

	// Ensure labels (once). Deduplicate to avoid PK violations.
	var labelIDs []int64
	seenLabel := make(map[string]bool)
	for _, lbl := range opts.Labels {
		lbl = strings.TrimSpace(lbl)
		if lbl == "" || seenLabel[lbl] {
			continue
		}
		seenLabel[lbl] = true
		labelID, err := st.EnsureLabel(src.ID, lbl, lbl, "user")
		if err != nil {
			failSync(err.Error())
			return nil, fmt.Errorf("ensure label %q: %w", lbl, err)
		}
		labelIDs = append(labelIDs, labelID)
	}

	// Save an initial checkpoint so the active sync records its inputs even
	// if interrupted before the first periodic checkpoint.
	if err := saveEmlCheckpoint(st, syncID, roots, startAfter, &cp); err != nil {
		cp.ErrorsCount++
		summary.Errors++
		log.Warn("failed to save initial checkpoint", "error", err)
	}

	lastCpFile := startAfter
	checkpointBlocked := false
	hardErrors := false

	type pendingEmlMsg struct {
		Raw       []byte
		RawHash   string
		SourceMsg string
		LabelIDs  []int64
		File      string
	}

	const (
		batchSize  = 200
		batchBytes = 32 << 20 // 32 MiB
	)

	var pending []pendingEmlMsg
	var pendingBytes int64
	pendingIdx := make(map[string]int) // SourceMsg → index in pending

	// flushPending writes the buffered batch and returns true when the
	// context was cancelled mid-flush so the caller can stop. Per-batch
	// errors are recorded on the summary and logged, never propagated.
	flushPending := func() bool {
		if len(pending) == 0 {
			return false
		}

		ids := make([]string, len(pending))
		for i, p := range pending {
			ids[i] = p.SourceMsg
		}

		existingWithRaw, err := st.MessageExistsWithRawBatch(src.ID, ids)
		batchOK := err == nil
		if err != nil {
			cp.ErrorsCount++
			summary.Errors++
			log.Warn("existence check failed", "error", err)
		}

		existingAny, err := st.MessageExistsBatch(src.ID, ids)
		anyOK := err == nil
		if err != nil {
			cp.ErrorsCount++
			summary.Errors++
			log.Warn("existence check failed (any)", "error", err)
		}

		for _, p := range pending {
			if err := ctx.Err(); err != nil {
				summary.Duration = time.Since(start)
				if err := saveEmlCheckpoint(
					st, syncID, roots, lastCpFile, &cp,
				); err != nil {
					cp.ErrorsCount++
					summary.Errors++
					log.Warn("failed to save checkpoint", "error", err)
				}
				return true
			}

			cp.MessagesProcessed++
			summary.MessagesProcessed++
			summary.BytesProcessed += int64(len(p.Raw))

			exists := false
			var existingID int64
			if batchOK {
				existingID, exists = existingWithRaw[p.SourceMsg]
			} else {
				one, err := st.MessageExistsWithRawBatch(
					src.ID, []string{p.SourceMsg},
				)
				if err != nil {
					cp.ErrorsCount++
					summary.Errors++
					log.Warn("existence check failed; attempting ingest anyway", "error", err)
				} else {
					existingID, exists = one[p.SourceMsg]
				}
			}

			if exists {
				summary.MessagesSkipped++
				// Add labels to existing message (same pattern as
				// mbox/emlx importers).
				if len(p.LabelIDs) > 0 && existingID > 0 {
					if err := st.AddMessageLabels(existingID, p.LabelIDs); err != nil {
						log.Warn("failed to add labels to existing message",
							"message_id", existingID, "error", err)
					} else {
						summary.LabelsUpdated++
					}
				}
				if !checkpointBlocked {
					lastCpFile = p.File
					checkpointIfDue(
						&cp, summary, opts.CheckpointInterval,
						st, syncID, roots, lastCpFile, log,
					)
				}
				continue
			}

			alreadyExists := false
			if anyOK {
				_, alreadyExists = existingAny[p.SourceMsg]
			}

			if err := ingestFn(
				ctx, st, src.ID, opts.Identifier,
				opts.AttachmentsDir, p.LabelIDs,
				p.SourceMsg, p.RawHash,
				p.Raw, time.Time{}, log,
			); err != nil {
				cp.ErrorsCount++
				summary.Errors++
				log.Warn("failed to ingest message",
					"source_msg", p.SourceMsg,
					"file", p.File,
					"error", err,
				)
				checkpointBlocked = true
				hardErrors = true
				continue
			}

			if alreadyExists {
				cp.MessagesUpdated++
				summary.MessagesUpdated++
			} else {
				cp.MessagesAdded++
				summary.MessagesAdded++
			}

			if !checkpointBlocked {
				lastCpFile = p.File
				checkpointIfDue(
					&cp, summary, opts.CheckpointInterval,
					st, syncID, roots, lastCpFile, log,
				)
			}
		}

		clear(pending)
		pending = pending[:0]
		pendingBytes = 0
		clear(pendingIdx)
		return false
	}

	for _, filePath := range files {
		if ctx.Err() != nil {
			break
		}

		// Resume: skip files already processed.
		if startAfter != "" && filePath <= startAfter {
			continue
		}

		fi, statErr := os.Stat(filePath)
		if statErr != nil {
			cp.ErrorsCount++
			summary.Errors++
			log.Warn("failed to stat .eml", "file", filePath, "error", statErr)
			continue
		}
		// For plain .eml the on-disk size bounds the read; .eml.gz is
		// bounded during decompression below.
		if !isGzip(filePath) && fi.Size() > opts.MaxMessageBytes {
			cp.ErrorsCount++
			summary.Errors++
			log.Warn("file exceeds size limit",
				"file", filePath, "size", fi.Size(),
				"limit", opts.MaxMessageBytes)
			continue
		}

		raw, err := readEMLFileLimited(filePath, opts.MaxMessageBytes)
		if err != nil {
			cp.ErrorsCount++
			summary.Errors++
			log.Warn("failed to read .eml", "file", filePath, "error", err)
			continue
		}

		sum := sha256.Sum256(raw)
		rawHash := hex.EncodeToString(sum[:])
		sourceMsgID := "eml-" + rawHash

		if idx, dup := pendingIdx[sourceMsgID]; dup {
			// Identical content already buffered this batch; merge labels.
			existing := pending[idx].LabelIDs
			for _, lid := range labelIDs {
				if !slices.Contains(existing, lid) {
					existing = append(existing, lid)
				}
			}
			pending[idx].LabelIDs = existing
		} else {
			pendingIdx[sourceMsgID] = len(pending)
			pending = append(pending, pendingEmlMsg{
				Raw:       raw,
				RawHash:   rawHash,
				SourceMsg: sourceMsgID,
				LabelIDs:  labelIDs,
				File:      filePath,
			})
			pendingBytes += int64(len(raw))
		}

		if len(pending) >= batchSize || pendingBytes >= batchBytes {
			if flushPending() {
				return summary, nil
			}
		}
	}

	if flushPending() {
		return summary, nil
	}

	summary.Duration = time.Since(start)
	summary.HardErrors = hardErrors

	// Final checkpoint.
	if err := saveEmlCheckpoint(st, syncID, roots, lastCpFile, &cp); err != nil {
		cp.ErrorsCount++
		summary.Errors++
		log.Warn("failed to save final checkpoint", "error", err)
	}

	// If cancelled, leave the sync run "running" so resume works.
	if ctx.Err() != nil {
		return summary, nil //nolint:nilerr // cancellation is signalled via summary, not error
	}

	if hardErrors {
		if err := st.FailSync(syncID, fmt.Sprintf(
			"completed with %d errors", cp.ErrorsCount,
		)); err != nil {
			return summary, fmt.Errorf("fail sync: %w", err)
		}
		return summary, nil
	}

	finalMsg := fmt.Sprintf("files:%d messages:%d", summary.FilesFound, summary.MessagesAdded)
	if cp.ErrorsCount > 0 {
		finalMsg = fmt.Sprintf(
			"files:%d messages:%d errors:%d",
			summary.FilesFound, summary.MessagesAdded, cp.ErrorsCount,
		)
	}
	if err := st.CompleteSync(syncID, finalMsg); err != nil {
		return summary, fmt.Errorf("complete sync: %w", err)
	}

	return summary, nil
}

// normalizedRoots returns the cleaned, absolute, sorted set of import roots
// used to key resume checkpoints.
func normalizedRoots(paths []string) []string {
	roots := make([]string, 0, len(paths))
	seen := make(map[string]bool)
	for _, p := range paths {
		abs, err := filepath.Abs(p)
		if err != nil {
			abs = filepath.Clean(p)
		}
		if seen[abs] {
			continue
		}
		seen[abs] = true
		roots = append(roots, abs)
	}
	sort.Strings(roots)
	return roots
}

func saveEmlCheckpoint(
	st *store.Store, syncID int64,
	roots []string, lastFile string, cp *store.Checkpoint,
) error {
	b, err := json.Marshal(emlCheckpoint{Roots: roots, LastFile: lastFile})
	if err != nil {
		return fmt.Errorf("marshal checkpoint: %w", err)
	}
	cp.PageToken = string(b)
	return st.UpdateSyncCheckpoint(syncID, cp)
}

func checkpointIfDue(
	cp *store.Checkpoint, summary *EmlImportSummary, interval int,
	st *store.Store, syncID int64,
	roots []string, lastFile string, log *slog.Logger,
) {
	if cp.MessagesProcessed%int64(interval) != 0 {
		return
	}
	if err := saveEmlCheckpoint(st, syncID, roots, lastFile, cp); err != nil {
		cp.ErrorsCount++
		summary.Errors++
		log.Warn("failed to save checkpoint", "error", err)
	}
}

// discoverFiles walks the given paths and returns all .eml and .eml.gz files.
func discoverFiles(paths []string, recursive bool) ([]string, error) {
	var files []string

	for _, path := range paths {
		info, err := os.Stat(path)
		if err != nil {
			return nil, fmt.Errorf("stat %s: %w", path, err)
		}

		if !info.IsDir() {
			if isEMLFile(path) {
				files = append(files, path)
			}
			continue
		}

		if recursive {
			err := filepath.WalkDir(path, func(p string, d os.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if !d.IsDir() && isEMLFile(p) {
					files = append(files, p)
				}
				return nil
			})
			if err != nil {
				return nil, fmt.Errorf("walk %s: %w", path, err)
			}
		} else {
			entries, err := os.ReadDir(path)
			if err != nil {
				return nil, fmt.Errorf("read dir %s: %w", path, err)
			}
			for _, entry := range entries {
				p := filepath.Join(path, entry.Name())
				if !entry.IsDir() && isEMLFile(p) {
					files = append(files, p)
				}
			}
		}
	}

	return files, nil
}

// isEMLFile returns true if the path has an .eml or .eml.gz extension.
func isEMLFile(path string) bool {
	lower := strings.ToLower(path)
	return strings.HasSuffix(lower, ".eml") || strings.HasSuffix(lower, ".eml.gz")
}

// isGzip returns true if the path has an .eml.gz (gzip) extension.
func isGzip(path string) bool {
	return strings.HasSuffix(strings.ToLower(path), ".eml.gz")
}

// readEMLFileLimited reads a .eml or .eml.gz file and returns the raw bytes,
// refusing to read more than maxBytes of (uncompressed) content.
func readEMLFileLimited(path string, maxBytes int64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()

	var r io.Reader = f
	if isGzip(path) {
		gz, err := gzip.NewReader(f)
		if err != nil {
			return nil, fmt.Errorf("gzip open: %w", err)
		}
		defer func() { _ = gz.Close() }()
		r = gz
	}

	// Read up to maxBytes+1 so we can detect oversize input.
	limited := io.LimitReader(r, maxBytes+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxBytes {
		return nil, fmt.Errorf("message exceeds size limit of %d bytes", maxBytes)
	}
	return data, nil
}

// readEMLFile reads a .eml or .eml.gz file and returns the raw bytes using
// the default size limit. Retained for tests and simple callers.
func readEMLFile(path string) ([]byte, error) {
	return readEMLFileLimited(path, defaultMaxEmlMessageBytes)
}
