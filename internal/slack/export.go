package slack

import (
	"archive/zip"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"go.kenn.io/msgvault/internal/store"
)

// ExportOptions configures an ImportExport run.
type ExportOptions struct {
	// UserID is the archiving user's Slack ID. It marks that user's own
	// messages is_from_me and forms the source identifier "<team>:<user>",
	// so an export shares the source row with add-slack/sync-slack when it
	// matches. Empty defaults to the sentinel "export" (nothing from-me).
	UserID string
	// Progress, if non-nil, is called after each conversation with a
	// human-readable status line.
	Progress func(string)
}

// ImportExport ingests an official Slack "Export data" archive (a .zip or an
// unpacked directory) into the same store the live API sync writes to.
//
// The archive layout is Slack's standard export format: top-level users.json,
// channels.json, groups.json, mpims.json and dms.json cataloging the
// workspace, plus one folder per conversation holding per-day message JSON
// arrays (YYYY-MM-DD.json). Each message is persisted through the identical
// path as sync-slack — same source_type ("slack"), same source identity
// ("<team>:<user>"), same source_message_id ("<channel>:<ts>"), same
// message_type ("slack") and raw format ("slack_json") — so an export imported
// for a workspace already synced via add-slack/sync-slack dedupes in place and
// both paths converge on identical rows.
//
// The Importer must be constructed with the workspace's team ID (NewImporter),
// but needs no Client: the export path makes no API calls. Files referenced by
// messages are flagged (has_attachments) but not downloaded here — the export
// carries url_private links that still require auth; media backfill is a
// follow-up, matching sync-slack's --no-media semantics.
func (imp *Importer) ImportExport(ctx context.Context, src string, opts ExportOptions) (*ImportSummary, error) {
	r, err := openExport(src)
	if err != nil {
		return nil, err
	}
	defer func() { _ = r.Close() }()

	sum := &ImportSummary{}
	start := time.Now()

	// Seed the identity resolver from the export's own users.json (the live
	// path fetches users.list from the API; here it is in the archive).
	if data, ok, uerr := r.top("users.json"); uerr != nil {
		return sum, fmt.Errorf("read users.json: %w", uerr)
	} else if ok {
		var users []User
		if jerr := json.Unmarshal(data, &users); jerr != nil {
			return sum, fmt.Errorf("parse users.json: %w", jerr)
		}
		for _, u := range users {
			imp.res.users[u.ID] = u
		}
	}

	teamID := imp.res.teamID
	if teamID == "" {
		return sum, errors.New("importer constructed without a team ID")
	}
	meUserID := opts.UserID
	if meUserID == "" {
		// A workspace export is not tied to one archiving user; a sentinel
		// keeps the source identifier stable and marks nothing is_from_me.
		meUserID = "export"
	}

	source, err := imp.store.GetOrCreateSource(sourceTypeSlack, teamID+":"+meUserID)
	if err != nil {
		return sum, fmt.Errorf("get/create slack source: %w", err)
	}
	sum.SourceID = source.ID
	if derr := imp.store.UpdateSourceDisplayName(source.ID, "Slack "+teamID+" (export)"); derr != nil {
		return sum, derr
	}

	convs, err := loadExportCatalog(r, meUserID)
	if err != nil {
		return sum, err
	}

	for i := range convs {
		if ctx.Err() != nil {
			break
		}
		ec := &convs[i]
		convID, cerr := imp.store.EnsureConversationWithType(
			source.ID, ec.conv.ID, conversationType(&ec.conv), conversationTitle(&ec.conv, imp.res.displayName))
		if cerr != nil {
			return sum, fmt.Errorf("ensure conversation %s: %w", ec.conv.ID, cerr)
		}
		toRecipients, merr := imp.exportMembership(convID, &ec.conv, ec.members, meUserID)
		if merr != nil {
			return sum, merr
		}

		days, derr := r.days(ec.folder)
		if derr != nil {
			return sum, derr
		}
		if len(days) == 0 && ec.altFolder != "" && ec.altFolder != ec.folder {
			if alt, aerr := r.days(ec.altFolder); aerr == nil && len(alt) > 0 {
				days, ec.folder = alt, ec.altFolder
			}
		}
		// Files are per-day and named YYYY-MM-DD.json, so ascending name order
		// is chronological: thread roots reach the archive before replies in
		// later day-files (SetReplyTo resolves to NULL harmlessly otherwise).
		for _, day := range days {
			if ctx.Err() != nil {
				break
			}
			data, rerr := r.read(ec.folder, day)
			if rerr != nil {
				return sum, rerr
			}
			var msgs []Message
			if jerr := json.Unmarshal(data, &msgs); jerr != nil {
				// One malformed day-file must not abort a multi-year import.
				sum.Errors++
				continue
			}
			for j := range msgs {
				if perr := imp.persistExportMessage(convID, source.ID, ec.conv.ID, &msgs[j], meUserID, toRecipients, sum); perr != nil {
					return sum, perr
				}
			}
		}
		sum.ConversationsProcessed++
		if opts.Progress != nil {
			opts.Progress(fmt.Sprintf("%s: %d messages",
				conversationTitle(&ec.conv, imp.res.displayName), sum.MessagesProcessed))
		}
	}

	sum.Duration = time.Since(start)
	return sum, nil
}

// exportMembership records conversation membership from the export catalog's
// member lists (the live path lists members via the API). It mirrors
// ensureMembership: DMs/group DMs also yield the per-message "to" recipient
// set; channels return a non-nil empty set (deliberately clearing any stale
// "to" fanout, matching the live importer's channel semantics).
func (imp *Importer) exportMembership(convID int64, c *Conversation, members []string, meUserID string) ([]messageRecipient, error) {
	var refs []store.ConversationParticipantRef
	directRecipients := make([]messageRecipient, 0)
	seen := map[string]bool{}
	add := func(userID string) error {
		if userID == "" || seen[userID] {
			return nil
		}
		seen[userID] = true
		pid, err := imp.res.resolveID(userID)
		if err != nil {
			return err
		}
		if pid != 0 {
			refs = append(refs, store.ConversationParticipantRef{ParticipantID: pid, Role: "member"})
			if c.IsIM || c.IsMpim {
				directRecipients = append(directRecipients, messageRecipient{id: pid, name: imp.res.displayName(userID)})
			}
		}
		return nil
	}
	for _, uid := range members {
		if err := add(uid); err != nil {
			return nil, err
		}
	}
	if c.IsIM {
		// Ensure both peers are present even if the catalog omitted a member.
		if err := add(c.User); err != nil {
			return nil, err
		}
		if meUserID != "export" {
			if err := add(meUserID); err != nil {
				return nil, err
			}
		}
	}
	if err := imp.store.ReplaceConversationParticipants(convID, refs); err != nil {
		return nil, err
	}
	return directRecipients, nil
}

// persistExportMessage persists one archived message and its auxiliary rows.
// It is the offline twin of processMessage: it reuses the exact mapper and the
// persistRecipients/persistReactions helpers, but takes no API client — no
// tombstone existence probe (exports carry no tombstones) and no file
// downloads.
func (imp *Importer) persistExportMessage(convID, sourceID int64, channelID string, m *Message, meUserID string, toRecipients []messageRecipient, sum *ImportSummary) error {
	if m.Type != "message" || m.TS == "" {
		return nil
	}
	raw := []byte(m.Raw)
	if len(raw) == 0 {
		return fmt.Errorf("slack export message %s:%s has no raw JSON", channelID, m.TS)
	}
	smid := sourceMessageID(channelID, m.TS)
	existing, err := imp.store.MessageExistsBatch(sourceID, []string{smid})
	if err != nil {
		return fmt.Errorf("check existing slack message: %w", err)
	}
	_, wasExisting := existing[smid]

	msg, text := mapMessage(m, channelID, convID, sourceID, m.User == meUserID, imp.res.displayName)

	var senderPID int64
	if m.User != "" {
		senderPID, err = imp.res.resolveID(m.User)
	} else if m.BotID != "" {
		senderPID, err = imp.res.resolveBot(m.BotID, m.Username)
	}
	if err != nil {
		return err
	}
	if senderPID != 0 {
		msg.SenderID = sql.NullInt64{Int64: senderPID, Valid: true}
	}

	messageID, err := imp.store.UpsertMessage(&msg)
	if err != nil {
		return err
	}
	if err := imp.store.UpsertMessageBody(messageID, sql.NullString{String: text, Valid: text != ""}, sql.NullString{}); err != nil {
		return err
	}
	// FTS is the one warn-and-continue write (derived, self-healing via
	// rebuild-fts), matching every other importer.
	if err := imp.store.UpsertFTS(messageID, "", text, imp.res.displayName(m.User), "", ""); err != nil {
		sum.Errors++
	}
	if m.Edited != nil {
		if err := imp.store.SetMessageEdited(messageID); err != nil {
			return fmt.Errorf("set message edited: %w", err)
		}
	}
	if err := imp.persistRecipients(messageID, m, senderPID, toRecipients); err != nil {
		return err
	}
	if err := imp.persistReactions(messageID, m); err != nil {
		return err
	}
	if m.IsThreadReply() {
		if err := imp.store.SetReplyTo(sourceID, smid, sourceMessageID(channelID, m.ThreadTS)); err != nil {
			return fmt.Errorf("link thread reply: %w", err)
		}
	}
	// Archive the exact original JSON last: row+raw is the completeness marker.
	if err := imp.store.UpsertMessageRawWithFormat(messageID, raw, "slack_json"); err != nil {
		return fmt.Errorf("archive slack message raw: %w", err)
	}

	if sum.processedMessageIDs == nil {
		sum.processedMessageIDs = make(map[string]struct{})
	}
	if _, counted := sum.processedMessageIDs[smid]; !counted {
		sum.processedMessageIDs[smid] = struct{}{}
		sum.MessagesProcessed++
		if wasExisting {
			sum.MessagesUpdated++
		} else {
			sum.MessagesAdded++
		}
	}
	return nil
}

// exportConv is one conversation discovered in the export catalog: its
// synthesized Conversation, the folder holding its day-files (with an
// alternate name to try — exports name channel folders by name and DM folders
// by ID, but conventions vary), and its member IDs.
type exportConv struct {
	conv      Conversation
	folder    string
	altFolder string
	members   []string
}

// loadExportCatalog reads the top-level catalog files and synthesizes the
// conversation list. Absent catalogs are skipped (a public-only export has no
// groups/mpims/dms).
func loadExportCatalog(r exportReader, meUserID string) ([]exportConv, error) {
	var out []exportConv

	// Public (channels.json) and private (groups.json) channels both map to
	// the "channel" conversation type.
	for _, spec := range []struct {
		file    string
		private bool
	}{{"channels.json", false}, {"groups.json", true}} {
		data, ok, err := r.top(spec.file)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		var arr []struct {
			ID         string   `json:"id"`
			Name       string   `json:"name"`
			Members    []string `json:"members"`
			IsArchived bool     `json:"is_archived"`
		}
		if err := json.Unmarshal(data, &arr); err != nil {
			return nil, fmt.Errorf("parse %s: %w", spec.file, err)
		}
		for _, e := range arr {
			out = append(out, exportConv{
				conv:      Conversation{ID: e.ID, Name: e.Name, IsChannel: true, IsPrivate: spec.private, IsArchived: e.IsArchived},
				folder:    e.Name,
				altFolder: e.ID,
				members:   e.Members,
			})
		}
	}

	if data, ok, err := r.top("mpims.json"); err != nil {
		return nil, err
	} else if ok {
		var arr []struct {
			ID      string   `json:"id"`
			Name    string   `json:"name"`
			Members []string `json:"members"`
		}
		if err := json.Unmarshal(data, &arr); err != nil {
			return nil, fmt.Errorf("parse mpims.json: %w", err)
		}
		for _, e := range arr {
			out = append(out, exportConv{
				conv:      Conversation{ID: e.ID, Name: e.Name, IsMpim: true},
				folder:    e.Name,
				altFolder: e.ID,
				members:   e.Members,
			})
		}
	}

	if data, ok, err := r.top("dms.json"); err != nil {
		return nil, err
	} else if ok {
		var arr []struct {
			ID      string   `json:"id"`
			Members []string `json:"members"`
		}
		if err := json.Unmarshal(data, &arr); err != nil {
			return nil, fmt.Errorf("parse dms.json: %w", err)
		}
		for _, e := range arr {
			out = append(out, exportConv{
				conv:      Conversation{ID: e.ID, IsIM: true, User: dmPeer(e.Members, meUserID)},
				folder:    e.ID,
				altFolder: "",
				members:   e.Members,
			})
		}
	}

	return out, nil
}

// dmPeer returns the DM's other party (the member that is not the archiving
// user); it falls back to the first member when "me" is unknown.
func dmPeer(members []string, meUserID string) string {
	for _, m := range members {
		if m != meUserID {
			return m
		}
	}
	if len(members) > 0 {
		return members[0]
	}
	return ""
}

// PeekExportTeamID reads the workspace team_id from an export's users.json so
// the caller can build the source identity before constructing an Importer.
func PeekExportTeamID(src string) (string, error) {
	r, err := openExport(src)
	if err != nil {
		return "", err
	}
	defer func() { _ = r.Close() }()
	data, ok, err := r.top("users.json")
	if err != nil {
		return "", err
	}
	if !ok {
		return "", errors.New("export is missing users.json")
	}
	var users []struct {
		TeamID string `json:"team_id"`
	}
	if err := json.Unmarshal(data, &users); err != nil {
		return "", fmt.Errorf("parse users.json: %w", err)
	}
	for _, u := range users {
		if u.TeamID != "" {
			return u.TeamID, nil
		}
	}
	return "", errors.New("no team_id found in users.json")
}

// exportReader abstracts a Slack export delivered as a .zip or an unpacked
// directory.
type exportReader interface {
	// top reads a top-level catalog file; ok is false when it is absent.
	top(name string) (data []byte, ok bool, err error)
	// days lists the message files in a conversation folder, ascending.
	days(folder string) ([]string, error)
	// read reads one message file previously returned by days.
	read(folder, name string) ([]byte, error)
	Close() error
}

// openExport opens a Slack export at src, which may be a .zip file or an
// already-unpacked directory.
func openExport(src string) (exportReader, error) {
	fi, err := os.Stat(src)
	if err != nil {
		return nil, err
	}
	if fi.IsDir() {
		return &dirReader{root: src}, nil
	}
	rc, err := zip.OpenReader(src)
	if err != nil {
		return nil, fmt.Errorf("open export zip: %w", err)
	}
	zr := &zipReader{rc: rc, files: make(map[string]*zip.File, len(rc.File))}
	for _, f := range rc.File {
		zr.files[f.Name] = f
	}
	// Tolerate a single wrapping directory (some zips nest everything under
	// one folder); detect it from users.json's location.
	if _, ok := zr.files["users.json"]; !ok {
		for name := range zr.files {
			if strings.HasSuffix(name, "/users.json") && strings.Count(name, "/") == 1 {
				zr.prefix = strings.TrimSuffix(name, "users.json")
				break
			}
		}
	}
	return zr, nil
}

type zipReader struct {
	rc     *zip.ReadCloser
	prefix string
	files  map[string]*zip.File
}

func (z *zipReader) top(name string) ([]byte, bool, error) {
	f, ok := z.files[z.prefix+name]
	if !ok {
		return nil, false, nil
	}
	b, err := readZipEntry(f)
	if err != nil {
		return nil, false, err
	}
	return b, true, nil
}

func (z *zipReader) days(folder string) ([]string, error) {
	pfx := z.prefix + folder + "/"
	var names []string
	for name := range z.files {
		if !strings.HasPrefix(name, pfx) {
			continue
		}
		rest := name[len(pfx):]
		if rest != "" && !strings.Contains(rest, "/") && strings.HasSuffix(rest, ".json") {
			names = append(names, rest)
		}
	}
	sort.Strings(names)
	return names, nil
}

func (z *zipReader) read(folder, name string) ([]byte, error) {
	f, ok := z.files[z.prefix+folder+"/"+name]
	if !ok {
		return nil, fmt.Errorf("export entry not found: %s/%s", folder, name)
	}
	return readZipEntry(f)
}

func (z *zipReader) Close() error { return z.rc.Close() }

func readZipEntry(f *zip.File) ([]byte, error) {
	rc, err := f.Open()
	if err != nil {
		return nil, err
	}
	defer func() { _ = rc.Close() }()
	return io.ReadAll(rc)
}

type dirReader struct{ root string }

func (d *dirReader) top(name string) ([]byte, bool, error) {
	b, err := os.ReadFile(filepath.Join(d.root, name))
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return b, true, nil
}

func (d *dirReader) days(folder string) ([]string, error) {
	entries, err := os.ReadDir(filepath.Join(d.root, folder))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var names []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".json") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	return names, nil
}

func (d *dirReader) read(folder, name string) ([]byte, error) {
	return os.ReadFile(filepath.Join(d.root, folder, name))
}

func (d *dirReader) Close() error { return nil }
