package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"go.kenn.io/msgvault/internal/importeml"
)

var (
	importEmlAccount            string
	importEmlSourceType         string
	importEmlRecursive          bool
	importEmlLabels             []string
	importEmlNoResume           bool
	importEmlCheckpointInterval int
	importEmlNoAttachments      bool
	noDefaultIdentityImportEml  bool
)

var importEMLCmd = &cobra.Command{
	Use:   "import-eml [flags] <path>...",
	Short: "Import .eml and .eml.gz files into the archive",
	Long: `Import email messages from .eml and .eml.gz files into msgvault.

Supports:
  - Plain .eml files (standard MIME format)
  - Gzip-compressed .eml.gz files (gmvault format)
  - GYB directory structures (gyb-*/YYYY/MM/DD/*.eml)
  - Gmvault directory structures (gmvault-*/db/YYYY-MM/*.eml.gz)

Messages are deduplicated by content hash, so re-importing is safe.

Examples:
  msgvault import-eml --account you@example.com message.eml
  msgvault import-eml --account you@example.com --recursive ./gyb-you/
  msgvault import-eml --account you@example.com --source-type gmvault --recursive ./gmvault-you/db/
  msgvault import-eml --account you@example.com --label archive --recursive ./archives/`,
	Args: cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		if importEmlAccount == "" {
			return errors.New("--account is required")
		}

		if !isDaemonCLISubprocess() {
			return runDaemonCLICommandHTTPFromCobra(cmd, args)
		}

		// Handle Ctrl+C gracefully (save checkpoint and exit cleanly).
		ctx, cancel := context.WithCancel(cmd.Context())
		defer cancel()
		sigChan := make(chan os.Signal, 2)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		done := make(chan struct{})
		defer func() {
			close(done)
			signal.Stop(sigChan)
			for {
				select {
				case <-sigChan:
					// Drain queued signals to avoid late os.Exit(130) during teardown.
				default:
					return
				}
			}
		}()
		go func() {
			signals := 0
			for {
				select {
				case <-done:
					return
				case <-sigChan:
					select {
					case <-done:
						return
					default:
					}
					signals++
					if signals == 1 {
						_, _ = fmt.Fprintln(cmd.ErrOrStderr(), "\nInterrupted. Saving checkpoint...")
						cancel()
						continue
					}
					_, _ = fmt.Fprintln(cmd.ErrOrStderr(), "Interrupted again. Exiting immediately.")
					os.Exit(130)
				}
			}
		}()

		st, cleanup, err := openWritableStoreAndInitForIngest()
		if err != nil {
			return err
		}
		defer cleanup()
		dbPath := cfg.DatabaseDSN()

		attachmentsDir := cfg.AttachmentsDir()
		if importEmlNoAttachments {
			attachmentsDir = ""
		}

		summary, err := importeml.ImportEmlPaths(ctx, st, args, importeml.EmlImportOptions{
			SourceType:         importEmlSourceType,
			Identifier:         importEmlAccount,
			Recursive:          importEmlRecursive,
			Labels:             importEmlLabels,
			NoResume:           importEmlNoResume,
			CheckpointInterval: importEmlCheckpointInterval,
			AttachmentsDir:     attachmentsDir,
			Logger:             logger,
		})
		if err != nil {
			return err
		}

		// Auto-default-identity must run BEFORE the legacy migration
		// retry — see comment in account_identity.go.
		if ctx.Err() == nil && !summary.HardErrors && !noDefaultIdentityImportEml {
			if summary.SourceID != 0 {
				confirmDefaultIdentity(cmd.OutOrStdout(), st, summary.SourceID, importEmlAccount, importEmlAccount, "account-identifier")
			} else {
				logger.Warn("auto-default-identity: missing source id", "identifier", importEmlAccount)
			}
		}

		if summary.SourceID != 0 {
			if err := runPostSourceCreateMigrations(st); err != nil {
				return fmt.Errorf("post-source-create migrations: %w", err)
			}
		}

		out := cmd.OutOrStdout()
		if ctx.Err() != nil {
			_, _ = fmt.Fprintln(out, "Import interrupted. Run again to resume.")
		} else if summary.Errors > 0 {
			_, _ = fmt.Fprintln(out, "Import complete (with errors).")
		} else {
			_, _ = fmt.Fprintln(out, "Import complete.")
		}
		_, _ = fmt.Fprintf(out, "  Files found:    %d\n", summary.FilesFound)
		_, _ = fmt.Fprintf(out, "  Processed:      %d messages\n", summary.MessagesProcessed)
		_, _ = fmt.Fprintf(out, "  Added:          %d messages\n", summary.MessagesAdded)
		_, _ = fmt.Fprintf(out, "  Updated:        %d messages\n", summary.MessagesUpdated)
		_, _ = fmt.Fprintf(out, "  Skipped (dup):  %d messages\n", summary.MessagesSkipped)
		_, _ = fmt.Fprintf(out, "  Labels updated: %d messages\n", summary.LabelsUpdated)
		_, _ = fmt.Fprintf(out, "  Errors:         %d\n", summary.Errors)
		_, _ = fmt.Fprintf(out, "  Bytes:          %.2f MB\n", float64(summary.BytesProcessed)/(1024*1024))

		cacheErr := rebuildCacheAfterWrite(dbPath)

		if ctx.Err() == nil && summary.HardErrors {
			return errors.Join(fmt.Errorf("import completed with %d errors", summary.Errors), cacheErr)
		}
		if ctx.Err() != nil {
			return errors.Join(context.Canceled, cacheErr)
		}
		return cacheErr
	},
}

func init() {
	rootCmd.AddCommand(importEMLCmd)

	importEMLCmd.Flags().StringVar(&importEmlAccount, "account", "", "email address to associate imported messages with (required)")
	importEMLCmd.Flags().StringVar(&importEmlSourceType, "source-type", "eml", "Source type to record in the database (e.g. eml, gmvault, gyb)")
	importEMLCmd.Flags().BoolVar(&importEmlRecursive, "recursive", false, "recursively scan directories for .eml/.eml.gz files")
	importEMLCmd.Flags().StringSliceVar(&importEmlLabels, "label", nil, "Label(s) to apply to imported messages (repeatable, or comma-separated)")
	importEMLCmd.Flags().BoolVar(&importEmlNoResume, "no-resume", false, "Do not resume from an interrupted import")
	importEMLCmd.Flags().IntVar(&importEmlCheckpointInterval, "checkpoint-interval", 200, "Save progress every N messages")
	importEMLCmd.Flags().BoolVar(&importEmlNoAttachments, "no-attachments", false, "Do not store attachments (disk or database). Messages will still be marked as having attachments.")
	importEMLCmd.Flags().BoolVar(&noDefaultIdentityImportEml, "no-default-identity", false, noDefaultIdentityHelp)

	_ = importEMLCmd.MarkFlagRequired("account")
}
