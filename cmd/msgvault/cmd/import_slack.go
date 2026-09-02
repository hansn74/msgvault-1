package cmd

import (
	"context"
	"errors"
	"fmt"

	"github.com/spf13/cobra"
	"go.kenn.io/msgvault/internal/slack"
)

var (
	importSlackMe   string
	importSlackTeam string
)

func newImportSlackCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "import-slack <export.zip|dir>",
		Short: "Import an official Slack export (channels, private channels, group DMs, DMs)",
		Long: `Import a Slack workspace export produced by Slack's "Export data" tool.

Accepts the standard export format (a .zip or an already-unpacked directory):
users.json / channels.json / groups.json / mpims.json / dms.json cataloging the
workspace, plus one folder per conversation of per-day message JSON.

Public channels, private channels, group DMs and DMs are ingested as
conversations. Messages dedupe by (channel_id, ts), so re-importing a later or
overlapping export is safe and incremental — an "Entire history" baseline
followed by weekly "Last 7 days" exports converges without duplicates. The
import lands on the same "slack" source as add-slack/sync-slack when --me is the
workspace user, so export-archived and API-synced rows merge.

Note: files/attachments are flagged but not downloaded (the export carries
auth-gated url_private links); message text, threads, reactions and membership
are fully imported.

Examples:
  msgvault import-slack ./slack-export.zip
  msgvault import-slack --me U03CWD3A5 ./slack-export/
  msgvault import-slack --team T03CWD3A3 ./export.zip`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if !isDaemonCLISubprocess() {
				return runDaemonCLICommandHTTPFromCobra(cmd, args)
			}
			src := args[0]

			teamID := importSlackTeam
			if teamID == "" {
				t, err := slack.PeekExportTeamID(src)
				if err != nil {
					return fmt.Errorf("determine workspace team id (pass --team to override): %w", err)
				}
				teamID = t
			}

			s, cleanup, err := openWritableStoreAndInitForIngest()
			if err != nil {
				return err
			}
			defer cleanup()

			ctx, stop := withInterruptCancel(cmd, "\nInterrupted. Progress is saved; re-run to continue.")
			defer stop()

			// No Client: the export path makes no Slack API calls.
			imp := slack.NewImporter(s, nil, teamID)
			sum, ierr := imp.ImportExport(ctx, src, slack.ExportOptions{
				UserID: importSlackMe,
				// Honour the same [slack] channel filters as sync-slack, so a
				// re-import cannot reinstate channels excluded from syncing.
				IncludeChannels: cfg.Slack.Channels,
				ExcludeChannels: cfg.Slack.ExcludeChannels,
				Progress:        func(line string) { writeSlackProgress(cmd.OutOrStdout(), line) },
			})

			// Rebuild analytics for whatever committed, even on interruption or
			// partial failure (matches the other importers).
			cacheErr := rebuildCacheAfterWrite(cfg.DatabaseDSN())
			if sum != nil && sum.SourceID != 0 {
				if merr := runPostSourceCreateMigrations(s); merr != nil {
					return errors.Join(fmt.Errorf("post-source-create migrations: %w", merr), cacheErr)
				}
			}
			if ierr != nil {
				return errors.Join(ierr, cacheErr)
			}
			if ctx.Err() != nil {
				_, _ = fmt.Fprintln(cmd.OutOrStdout(), "\nInterrupted — re-run import-slack to continue.")
				return errors.Join(context.Canceled, cacheErr)
			}
			printSlackExportSummary(cmd, teamID, sum)
			return cacheErr
		},
	}
	cmd.Flags().StringVar(&importSlackMe, "me", "", "your Slack user ID (marks your own messages, and shares the source with add-slack/sync-slack)")
	cmd.Flags().StringVar(&importSlackTeam, "team", "", "workspace team ID (default: read from the export's users.json)")
	return cmd
}

func printSlackExportSummary(cmd *cobra.Command, teamID string, sum *slack.ImportSummary) {
	if sum == nil {
		return
	}
	out := cmd.OutOrStdout()
	_, _ = fmt.Fprintf(out, "Slack export imported (%s):\n", teamID)
	_, _ = fmt.Fprintf(out, "  Conversations: %d\n", sum.ConversationsProcessed)
	_, _ = fmt.Fprintf(out, "  Processed:     %d messages\n", sum.MessagesProcessed)
	_, _ = fmt.Fprintf(out, "  Added:         %d\n", sum.MessagesAdded)
	_, _ = fmt.Fprintf(out, "  Updated (dup): %d\n", sum.MessagesUpdated)
	if sum.Errors > 0 {
		_, _ = fmt.Fprintf(out, "  Errors:        %d\n", sum.Errors)
	}
}

func init() {
	rootCmd.AddCommand(newImportSlackCmd())
}
