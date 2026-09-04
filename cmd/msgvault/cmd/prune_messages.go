package cmd

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"go.kenn.io/msgvault/internal/store"
)

const (
	pruneMessagesCommandName   = "prune-messages"
	pruneMessagesConfirmedFlag = "confirmed"

	// pruneMessagesProgressInterval throttles the progress line. Batches
	// commit far faster than a human reads, and this command can run for
	// hours over millions of rows.
	pruneMessagesProgressInterval = 5 * time.Second
)

// pruneMessagesOptions is the parsed flag set, kept separate from cobra so
// the plan resolver below is testable without a command harness.
type pruneMessagesOptions struct {
	sources       []string
	titleGlobs    []string
	botsOnly      bool
	batchSize     int
	deferFTS      bool
	dryRun        bool
	yes           bool
	confirmed     bool
	keepSource    bool
	deleteSources bool
}

// prunePlanEntry is one selector and the authorship split of what it matches.
// The counts describe the selector's whole scope, so the human number is
// readable both ways: with --bots-only it is what survives, without it is
// what gets destroyed.
type prunePlanEntry struct {
	Label  string
	Counts store.PruneMatchCounts
}

// prunePlan is the pre-flight summary printed before anything is deleted.
type prunePlan struct {
	Entries []prunePlanEntry
	// Scope is the UNION of the selectors, ignoring --bots-only. It is at
	// most the sum of the per-selector counts: a message matched by two
	// selectors is counted once here and once under each selector above.
	Scope store.PruneMatchCounts
	// Total is how many messages the run will actually delete: Scope.Bots
	// under --bots-only, Scope.Total otherwise.
	Total int64
	// BotsOnly echoes the flag so the printer can say which reading applies.
	BotsOnly bool
	// Filter is the resolved selection the prune run executes.
	Filter store.PruneFilter
}

func newPruneMessagesCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   pruneMessagesCommandName,
		Short: "Permanently delete messages from the local archive in bounded batches",
		Long: `Permanently delete messages from the LOCAL archive, in small batches that
each commit on their own.

This exists for deletions too large for the cascade delete behind
'remove-account' and 'delete-deduped'. Those wrap an unbounded DELETE in a
single transaction; on a multi-hundred-gigabyte archive that grows the WAL
without bound and may never reach COMMIT. Here every batch is durable as soon
as it lands, Ctrl-C costs at most one batch, and re-running the same command
resumes where the last run stopped.

Select what to delete with at least one of --source and --conversation-title;
multiple selectors are combined with OR. --conversation-title takes a glob
where '*' matches any run of characters.

--bots-only then narrows that selection (it ANDs, it does not OR) to messages
with no human author: the sender has no profile email address, which is how a
bot, webhook, or integration post differs from a person's. Use it when a
channel is a bot firehose that people nevertheless talk in — the pre-flight
summary reports the bot/human split per selector so you can see exactly how
many human-written messages a run would take or spare.

Messages go with everything hanging off them: bodies, raw MIME, recipients,
labels, reactions, attachment ROWS, and full-text search entries.
Conversations left empty are removed too, and a --source whose messages are
all gone loses its account row unless --keep-source is given.

ATTACHMENT FILES ON DISK ARE NOT DELETED. Only the database rows go. The
content-addressed blobs stay where they are until attachment maintenance
reclaims them.

The Parquet analytics cache is rebuilt at the end. The vector index is not;
rebuild it separately with 'msgvault embeddings build --full-rebuild'.

This is irreversible. Back up first, and use --dry-run to see the counts.

Examples:
  msgvault prune-messages --conversation-title '#logs-*' --dry-run
  msgvault prune-messages --conversation-title '#logs-*' --bots-only
  msgvault prune-messages --conversation-title '#logs-*' --batch-size 5000
  msgvault prune-messages --source gmail:noreply@example.com --yes
  msgvault prune-messages --source mbox:archive@example.com --keep-source`,
		Args: cobra.NoArgs,
		RunE: runPruneMessages,
	}

	cmd.Flags().StringArray("source", nil,
		"Delete messages of this source, as <type:identifier> (repeatable)")
	cmd.Flags().StringArray("conversation-title", nil,
		"Delete messages whose conversation title matches this glob (repeatable)")
	cmd.Flags().Bool("bots-only", false,
		"Delete only messages with no human author (sender has no email address)")
	cmd.Flags().Int("batch-size", store.DefaultPruneBatchSize,
		"Messages deleted per committed transaction")
	cmd.Flags().Bool("defer-fts", false,
		"Skip the per-batch full-text delete; run 'msgvault rebuild-fts' afterwards")
	cmd.Flags().Bool("dry-run", false,
		"Report what would be deleted and change nothing")
	cmd.Flags().BoolP("yes", "y", false, "Skip confirmation prompt")
	cmd.Flags().Bool("keep-source", false,
		"Keep the sources row after deleting a --source account's messages")
	cmd.Flags().Bool(pruneMessagesConfirmedFlag, false,
		"Internal: confirmation was already accepted by the frontend CLI")
	if err := cmd.Flags().MarkHidden(pruneMessagesConfirmedFlag); err != nil {
		panic(err)
	}
	return cmd
}

func runPruneMessages(cmd *cobra.Command, args []string) error {
	if !isDaemonCLISubprocess() {
		return runPruneMessagesHTTP(cmd, args)
	}
	return runPruneMessagesLocal(cmd, args)
}

// runPruneMessagesHTTP is the frontend half of the daemon-routed run. The
// daemon subprocess has no stdin, so — exactly as remove-account does — the
// confirmation is taken here and forwarded as a hidden flag. The selector
// summary is printed by the subprocess just before it starts deleting, so the
// prompt names the selectors rather than the counts; --dry-run is the way to
// see counts before committing to a run.
func runPruneMessagesHTTP(cmd *cobra.Command, args []string) error {
	opts, err := pruneMessagesFlags(cmd)
	if err != nil {
		return err
	}
	if err := validatePruneSelectors(cmd, opts); err != nil {
		return err
	}
	if !opts.dryRun && !opts.yes && !opts.confirmed {
		ok, err := confirmPruneMessages(cmd.InOrStdin(), cmd.OutOrStdout(), opts)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		if err := cmd.Flags().Set(pruneMessagesConfirmedFlag, "true"); err != nil {
			return fmt.Errorf("set --%s after confirmation: %w", pruneMessagesConfirmedFlag, err)
		}
	}
	return runDaemonCLICommandHTTPFromCobra(cmd, args)
}

func runPruneMessagesLocal(cmd *cobra.Command, _ []string) error {
	opts, err := pruneMessagesFlags(cmd)
	if err != nil {
		return err
	}
	if err := validatePruneSelectors(cmd, opts); err != nil {
		return err
	}

	s, cleanup, err := openWritableStoreAndInit()
	if err != nil {
		return err
	}
	defer cleanup()

	ctx, stop := withInterruptCancel(cmd,
		"\nInterrupted; finishing the current batch. Re-run to resume.")
	defer stop()

	out := cmd.OutOrStdout()
	plan, err := resolvePrunePlan(ctx, s, opts)
	if err != nil {
		return err
	}
	printPrunePlan(out, plan)

	if plan.Total == 0 {
		_, _ = fmt.Fprintln(out, "Nothing to delete.")
		return nil
	}
	if opts.dryRun {
		_, _ = fmt.Fprintln(out, "\nDry run: nothing was deleted.")
		return nil
	}
	if !opts.yes && !opts.confirmed {
		ok, err := confirmPruneMessages(cmd.InOrStdin(), out, opts)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
	}

	result, pruneErr := s.PruneMessagesContext(ctx, plan.Filter, store.PruneOptions{
		BatchSize:          opts.batchSize,
		DeferFTS:           opts.deferFTS,
		Total:              plan.Total,
		Progress:           newPruneProgressPrinter(cmd.ErrOrStderr(), plan.Total),
		DeleteEmptySources: opts.deleteSources,
	})
	printPruneResult(out, result)

	// Rebuild even after a failure: whatever committed before it is already
	// missing from the archive, so the cache is stale either way.
	cacheErr := rebuildCacheAfterWrite(cfg.DatabaseDSN())
	if cacheErr != nil {
		cacheErr = fmt.Errorf("rebuild analytics cache after prune: %w", cacheErr)
	}
	return errors.Join(pruneErr, cacheErr)
}

// pruneMessagesFlags reads the flag set into a plain struct.
func pruneMessagesFlags(cmd *cobra.Command) (pruneMessagesOptions, error) {
	var opts pruneMessagesOptions
	var err error
	if opts.sources, err = cmd.Flags().GetStringArray("source"); err != nil {
		return opts, fmt.Errorf("read --source flag: %w", err)
	}
	if opts.titleGlobs, err = cmd.Flags().GetStringArray("conversation-title"); err != nil {
		return opts, fmt.Errorf("read --conversation-title flag: %w", err)
	}
	if opts.botsOnly, err = cmd.Flags().GetBool("bots-only"); err != nil {
		return opts, fmt.Errorf("read --bots-only flag: %w", err)
	}
	if opts.batchSize, err = cmd.Flags().GetInt("batch-size"); err != nil {
		return opts, fmt.Errorf("read --batch-size flag: %w", err)
	}
	if opts.deferFTS, err = cmd.Flags().GetBool("defer-fts"); err != nil {
		return opts, fmt.Errorf("read --defer-fts flag: %w", err)
	}
	if opts.dryRun, err = cmd.Flags().GetBool("dry-run"); err != nil {
		return opts, fmt.Errorf("read --dry-run flag: %w", err)
	}
	if opts.yes, err = cmd.Flags().GetBool("yes"); err != nil {
		return opts, fmt.Errorf("read --yes flag: %w", err)
	}
	if opts.keepSource, err = cmd.Flags().GetBool("keep-source"); err != nil {
		return opts, fmt.Errorf("read --keep-source flag: %w", err)
	}
	if opts.confirmed, err = cmd.Flags().GetBool(pruneMessagesConfirmedFlag); err != nil {
		return opts, fmt.Errorf("read --%s flag: %w", pruneMessagesConfirmedFlag, err)
	}
	opts.deleteSources = !opts.keepSource
	return opts, nil
}

func validatePruneSelectors(cmd *cobra.Command, opts pruneMessagesOptions) error {
	if len(opts.sources) == 0 && len(opts.titleGlobs) == 0 {
		return usageErr(cmd, errors.New(
			"must specify at least one --source or --conversation-title"))
	}
	if opts.batchSize <= 0 {
		return usageErr(cmd, errors.New("--batch-size must be positive"))
	}
	for _, spec := range opts.sources {
		if _, _, err := splitPruneSourceSpec(spec); err != nil {
			return usageErr(cmd, err)
		}
	}
	return nil
}

// splitPruneSourceSpec splits "<type>:<identifier>" on the FIRST colon, so
// identifiers that contain colons themselves (a Slack "<team>:<user>", for
// one) survive intact.
func splitPruneSourceSpec(spec string) (sourceType, identifier string, err error) {
	sourceType, identifier, found := strings.Cut(spec, ":")
	if !found || sourceType == "" || identifier == "" {
		return "", "", fmt.Errorf(
			"invalid --source %q: expected <type:identifier>, e.g. gmail:you@example.com", spec)
	}
	return sourceType, identifier, nil
}

// resolvePrunePlan turns the selectors into a store filter and counts each
// one separately so the user sees which selector is responsible for what.
func resolvePrunePlan(
	ctx context.Context, s *store.Store, opts pruneMessagesOptions,
) (prunePlan, error) {
	var plan prunePlan
	plan.BotsOnly = opts.botsOnly
	plan.Filter.BotsOnly = opts.botsOnly

	for _, spec := range opts.sources {
		sourceType, identifier, err := splitPruneSourceSpec(spec)
		if err != nil {
			return plan, err
		}
		source, err := resolvePruneSource(s, sourceType, identifier)
		if err != nil {
			return plan, err
		}
		plan.Filter.SourceIDs = append(plan.Filter.SourceIDs, source.ID)

		counts, err := s.CountPruneMatchesSplit(ctx, store.PruneFilter{SourceIDs: []int64{source.ID}})
		if err != nil {
			return plan, err
		}
		plan.Entries = append(plan.Entries, prunePlanEntry{
			Label:  fmt.Sprintf("source %s:%s", source.SourceType, source.Identifier),
			Counts: counts,
		})
	}

	for _, glob := range opts.titleGlobs {
		pattern := store.GlobToLikePattern(glob)
		plan.Filter.TitlePatterns = append(plan.Filter.TitlePatterns, pattern)

		counts, err := s.CountPruneMatchesSplit(ctx, store.PruneFilter{TitlePatterns: []string{pattern}})
		if err != nil {
			return plan, err
		}
		plan.Entries = append(plan.Entries, prunePlanEntry{
			Label:  "conversation title " + glob,
			Counts: counts,
		})
	}

	scope, err := s.CountPruneMatchesSplit(ctx, plan.Filter)
	if err != nil {
		return plan, err
	}
	plan.Scope = scope
	plan.Total = scope.Total
	if opts.botsOnly {
		plan.Total = scope.Bots
	}
	return plan, nil
}

// resolvePruneSource finds the one source with this type and identifier.
// sources carries UNIQUE(source_type, identifier), so a type-qualified
// lookup is never ambiguous — which is why --source is type-qualified.
func resolvePruneSource(s *store.Store, sourceType, identifier string) (*store.Source, error) {
	sources, err := s.GetSourcesByIdentifier(identifier)
	if err != nil {
		return nil, fmt.Errorf("look up source %s:%s: %w", sourceType, identifier, err)
	}
	for _, source := range sources {
		if source.SourceType == sourceType {
			return source, nil
		}
	}
	if len(sources) == 0 {
		return nil, fmt.Errorf("no source %q found", identifier)
	}
	types := make([]string, 0, len(sources))
	for _, source := range sources {
		types = append(types, source.SourceType)
	}
	return nil, fmt.Errorf(
		"no source %q with type %q (known types for this identifier: %s)",
		identifier, sourceType, strings.Join(types, ", "))
}

func printPrunePlan(w io.Writer, plan prunePlan) {
	_, _ = fmt.Fprintln(w, "Messages in scope, by selector:")
	for _, entry := range plan.Entries {
		printPruneCountsLine(w, entry.Label, entry.Counts)
	}
	printPruneCountsLine(w, "TOTAL (distinct messages)", plan.Scope)

	if plan.BotsOnly {
		_, _ = fmt.Fprintf(w,
			"\n--bots-only: will delete %s bot message(s); %s human-authored message(s) are kept.\n",
			formatCount(plan.Total), formatCount(plan.Scope.Humans))
		return
	}
	_, _ = fmt.Fprintf(w,
		"\nWill delete all %s message(s) in scope.\n", formatCount(plan.Total))
	if plan.Scope.Humans > 0 {
		_, _ = fmt.Fprintf(w,
			"WARNING: that includes %s human-authored message(s). "+
				"Add --bots-only to keep them.\n", formatCount(plan.Scope.Humans))
	}
}

func printPruneCountsLine(w io.Writer, label string, counts store.PruneMatchCounts) {
	_, _ = fmt.Fprintf(w, "  %-44s %12s  (bots %s / humans %s)\n",
		label, formatCount(counts.Total),
		formatCount(counts.Bots), formatCount(counts.Humans))
}

func printPruneResult(w io.Writer, result store.PruneResult) {
	_, _ = fmt.Fprintf(w, "\nDeleted %s message(s) in %s batch(es).\n",
		formatCount(result.MessagesDeleted), formatCount(result.Batches))
	if result.ConversationsDeleted > 0 {
		_, _ = fmt.Fprintf(w, "Removed %s now-empty conversation(s).\n",
			formatCount(result.ConversationsDeleted))
	}
	if result.SourcesDeleted > 0 {
		_, _ = fmt.Fprintf(w, "Removed %s emptied source(s).\n",
			formatCount(result.SourcesDeleted))
	}
	if result.Interrupted {
		_, _ = fmt.Fprintf(w,
			"\nStopped early. Every batch above is committed; re-run the same "+
				"command to continue.\n")
	}
	_, _ = fmt.Fprintln(w, "\nAttachment files on disk were not deleted.")
}

// confirmPruneMessages prompts for a y/N answer, naming the selectors so the
// prompt is meaningful even in the daemon-routed path where the counts have
// not been fetched yet.
func confirmPruneMessages(r io.Reader, w io.Writer, opts pruneMessagesOptions) (bool, error) {
	_, _ = fmt.Fprintln(w, "\nThis permanently deletes messages from the local archive.")
	for _, spec := range opts.sources {
		_, _ = fmt.Fprintf(w, "  source %s\n", spec)
	}
	for _, glob := range opts.titleGlobs {
		_, _ = fmt.Fprintf(w, "  conversation title %s\n", glob)
	}
	if opts.botsOnly {
		_, _ = fmt.Fprintln(w, "  restricted to messages with no human author (--bots-only)")
	}
	_, _ = fmt.Fprint(w, "Proceed? This is irreversible. [y/N] ")

	scanner := bufio.NewScanner(r)
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return false, fmt.Errorf("read confirmation: %w", err)
		}
		return false, errors.New("no confirmation input (stdin closed); use --yes")
	}
	if !isYesAnswer(strings.TrimSpace(strings.ToLower(scanner.Text()))) {
		_, _ = fmt.Fprintln(w, "Aborted.")
		return false, nil
	}
	return true, nil
}

// newPruneProgressPrinter returns a store.PruneOptions Progress callback that
// emits a throttled one-line summary: how far along, the sustained rate, and
// an ETA from that rate.
func newPruneProgressPrinter(w io.Writer, total int64) func(store.PruneProgress) {
	var lastPrint time.Time
	return func(p store.PruneProgress) {
		now := time.Now()
		if now.Sub(lastPrint) < pruneMessagesProgressInterval {
			return
		}
		lastPrint = now

		rate := float64(p.MessagesDeleted) / max(p.Elapsed.Seconds(), 0.001)
		if total <= 0 || rate <= 0 {
			_, _ = fmt.Fprintf(w, "pruned %s message(s) — %.0f msg/s\n",
				formatCount(p.MessagesDeleted), rate)
			return
		}
		remaining := max(total-p.MessagesDeleted, 0)
		eta := time.Duration(float64(remaining)/rate) * time.Second
		_, _ = fmt.Fprintf(w, "pruned %s/%s (%.1f%%) — %.0f msg/s, ETA %s\n",
			formatCount(p.MessagesDeleted), formatCount(total),
			100*float64(p.MessagesDeleted)/float64(total), rate,
			formatCLIProgressDuration(eta, cliProgressDurationSpaced))
	}
}

func init() {
	rootCmd.AddCommand(newPruneMessagesCmd())
}
