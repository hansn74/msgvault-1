package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"go.kenn.io/msgvault/internal/config"
	"go.kenn.io/msgvault/internal/store"
	"go.kenn.io/msgvault/internal/tldv"
)

var (
	syncTldvLimit int
	syncTldvAfter string
	syncTldvFull  bool
)

var (
	newTldvClient                      = tldv.NewClient
	rebuildTldvCacheAfterWrite         = rebuildCacheAfterWrite
	rebuildTldvCacheAfterScheduledSync = rebuildCacheAfterScheduledSync
)

const tldvConfigHint = `Add to your config.toml:

  [[tldv]]
  identifier = "you@example.com"    # label for this account
  account_email = "you@example.com" # primary identity for organizer attribution
  api_key = "..."                   # from tl;dv's API settings (sent as x-api-key)
  enabled = true
  # schedule = "0 */6 * * *"        # optional daemon schedule`

// resolveTldvSource picks the [[tldv]] entry for an optional CLI argument: an
// explicit identifier must match a configured entry; with no argument there
// must be exactly one entry.
func resolveTldvSource(args []string) (*config.TldvSource, error) {
	if len(cfg.Tldv) == 0 {
		return nil, errors.New("no [[tldv]] sources configured\n\n" + tldvConfigHint)
	}
	if len(args) > 0 {
		src := cfg.GetTldvSource(args[0])
		if src == nil {
			var ids []string
			for _, s := range cfg.Tldv {
				ids = append(ids, s.Identifier)
			}
			return nil, fmt.Errorf("no [[tldv]] entry with identifier %q (configured: %s)", args[0], strings.Join(ids, ", "))
		}
		return src, nil
	}
	if len(cfg.Tldv) > 1 {
		return nil, errors.New("multiple [[tldv]] sources configured; pass an identifier")
	}
	src := cfg.Tldv[0]
	return &src, nil
}

var addTldvCmd = &cobra.Command{
	Use:   "add-tldv [identifier]",
	Short: "Register a tl;dv account and validate its API key",
	Long: `Register a configured tl;dv account as a msgvault source.

Reads the API key from the matching [[tldv]] entry in config.toml and
validates it with a live API call. tl;dv API keys are created in tl;dv's API
settings and are sent as the x-api-key request header.

Examples:
  msgvault add-tldv
  msgvault add-tldv you@example.com`,
	Args: cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		if !isDaemonCLISubprocess() {
			return runDaemonCLICommandHTTPFromCobra(cmd, args)
		}

		src, err := resolveTldvSource(args)
		if err != nil {
			return err
		}
		accountEmail, err := src.EffectiveAccountEmail()
		if err != nil {
			return err
		}
		if src.APIKey == "" {
			return fmt.Errorf("[[tldv]] entry %q has no api_key\n\n%s", src.Identifier, tldvConfigHint)
		}

		// Live probe: one meeting is enough to prove the key works.
		client := tldv.NewClient(tldv.DefaultBaseURL, src.APIKey)
		if _, err := client.ListMeetings(cmd.Context(), tldv.ListMeetingsParams{Page: 1, Limit: 1}); err != nil {
			return fmt.Errorf("validate tl;dv API key: %w", err)
		}

		s, cleanup, err := openWritableStoreAndInitForIngest()
		if err != nil {
			return err
		}
		defer cleanup()

		if _, err := registerMeetingSource(
			cmd.OutOrStdout(), s, sourceTypeTldv, src.Identifier, accountEmail,
		); err != nil {
			return err
		}
		if err := runPostSourceCreateMigrations(s); err != nil {
			return fmt.Errorf("post-source-create migrations: %w", err)
		}

		_, _ = fmt.Fprintf(cmd.OutOrStdout(), "\ntl;dv account registered successfully!\n")
		_, _ = fmt.Fprintf(cmd.OutOrStdout(), "  Identifier: %s\n\n", src.Identifier)
		_, _ = fmt.Fprintln(cmd.OutOrStdout(), "You can now run:")
		_, _ = fmt.Fprintf(cmd.OutOrStdout(), "  msgvault sync-tldv %s\n", src.Identifier)
		return nil
	},
}

var syncTldvCmd = &cobra.Command{
	Use:   "sync-tldv [identifier]",
	Short: "Sync tl;dv meeting recordings and transcripts",
	Long: `Sync meeting recordings and transcripts from tl;dv.

Incremental by default: only meetings that happened since the last successful
run are fetched. With no identifier, every configured [[tldv]] source is
synced.

Use --full to ignore the stored cursor and re-fetch everything; --after
bounds a full sync to meetings that happened on or after the given date.
Re-fetched meetings are upserted in place, so --full repairs existing rows
without duplicates.

Examples:
  msgvault sync-tldv
  msgvault sync-tldv you@example.com --limit 5
  msgvault sync-tldv --full --after 2024-01-01`,
	Args: cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		if !isDaemonCLISubprocess() {
			return runDaemonCLICommandHTTPFromCobra(cmd, args)
		}

		var sources []config.TldvSource
		if len(args) > 0 || len(cfg.Tldv) == 1 {
			src, err := resolveTldvSource(args)
			if err != nil {
				return err
			}
			sources = []config.TldvSource{*src}
		} else {
			sources = cfg.Tldv
		}
		if len(sources) == 0 {
			return errors.New("no [[tldv]] sources configured\n\n" + tldvConfigHint)
		}

		var after time.Time
		if syncTldvAfter != "" {
			t, err := time.Parse("2006-01-02", syncTldvAfter)
			if err != nil {
				return usageErr(cmd, fmt.Errorf("invalid --after %q (expected YYYY-MM-DD): %w", syncTldvAfter, err))
			}
			after = t.UTC()
		}
		type validatedTldvSource struct {
			source       config.TldvSource
			accountEmail string
		}
		validatedSources := make([]validatedTldvSource, 0, len(sources))
		for _, src := range sources {
			accountEmail, err := src.EffectiveAccountEmail()
			if err != nil {
				return err
			}
			if src.APIKey == "" {
				return fmt.Errorf("[[tldv]] entry %q has no api_key", src.Identifier)
			}
			validatedSources = append(validatedSources, validatedTldvSource{
				source: src, accountEmail: accountEmail,
			})
		}

		s, cleanup, err := openWritableStoreAndInitForIngest()
		if err != nil {
			return err
		}
		defer cleanup()
		dbPath := cfg.DatabaseDSN()

		ctx, cancel := context.WithCancel(cmd.Context())
		defer cancel()
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		defer signal.Stop(sigChan)
		go func() {
			select {
			case <-sigChan:
				_, _ = fmt.Fprintln(cmd.ErrOrStderr(), "\nInterrupted. Finishing current meeting...")
				cancel()
			case <-ctx.Done():
			}
		}()

		pendingCacheWrites := &tldv.ImportSummary{}
		for _, validated := range validatedSources {
			src := validated.source
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Syncing tl;dv for %s\n\n", src.Identifier)

			imp := tldv.NewImporter(s, newTldvClient(tldv.DefaultBaseURL, src.APIKey))
			sum, err := imp.Import(ctx, tldv.ImportOptions{
				Identifier:   src.Identifier,
				AccountEmail: validated.accountEmail,
				Full:         syncTldvFull || !after.IsZero(),
				Limit:        syncTldvLimit,
				After:        after,
				Progress:     func(line string) { _, _ = fmt.Fprintln(cmd.OutOrStdout(), "  "+line) },
			})
			if sum != nil {
				pendingCacheWrites.NotesAdded += sum.NotesAdded
				pendingCacheWrites.NotesUpdated += sum.NotesUpdated
			}
			if ctx.Err() != nil {
				_, _ = fmt.Fprintln(cmd.OutOrStdout(), "\nInterrupted — re-run sync-tldv to resume.")
				return finishTldvImport(src.Identifier, pendingCacheWrites, ctx.Err(), func() error {
					return rebuildTldvCacheAfterWrite(dbPath)
				})
			}
			if finishErr := finishTldvImport(src.Identifier, pendingCacheWrites, err, func() error {
				return rebuildTldvCacheAfterWrite(dbPath)
			}); finishErr != nil {
				return finishErr
			}

			_, _ = fmt.Fprintln(cmd.OutOrStdout())
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), "tl;dv sync complete!")
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "  Duration:           %s\n", sum.Duration.Round(time.Second))
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "  Meetings processed: %d\n", sum.NotesProcessed)
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "  Meetings added:     %d\n", sum.NotesAdded)
			if sum.Errors > 0 {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "  Errors:             %d\n", sum.Errors)
			}
		}

		return rebuildTldvCacheAfterWrite(dbPath)
	},
}

func finishTldvImport(
	identifier string,
	sum *tldv.ImportSummary,
	importErr error,
	refreshCache func() error,
) error {
	if importErr == nil {
		return nil
	}
	var refreshErr error
	if sum != nil && sum.NotesAdded+sum.NotesUpdated > 0 && refreshCache != nil {
		refreshErr = refreshCache()
	}
	return errors.Join(fmt.Errorf("tldv sync %s failed: %w", identifier, importErr), refreshErr)
}

// runConfiguredTldvSync is the daemon-scheduler entry point for one [[tldv]]
// source.
func runConfiguredTldvSync(ctx context.Context, st *store.Store, src config.TldvSource) error {
	refreshCtx := context.WithoutCancel(ctx)
	// Generic scheduler jobs and mutating daemon requests share the operation
	// gate, so a registered source cannot be removed between this precheck and
	// the importer's existing-source GetOrCreateSource call.
	registered, err := st.ListSources(tldv.SourceType)
	if err != nil {
		return fmt.Errorf("list registered tl;dv sources: %w", err)
	}
	found := false
	for _, candidate := range registered {
		if candidate.Identifier == src.Identifier {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("tldv source %q is not registered; run msgvault add-tldv %s",
			src.Identifier, src.Identifier)
	}
	if src.APIKey == "" {
		return fmt.Errorf("tldv source %q has no api_key", src.Identifier)
	}
	accountEmail, err := src.EffectiveAccountEmail()
	if err != nil {
		return err
	}
	imp := tldv.NewImporter(st, newTldvClient(tldv.DefaultBaseURL, src.APIKey))
	sum, err := imp.Import(ctx, tldv.ImportOptions{
		Identifier:   src.Identifier,
		AccountEmail: accountEmail,
	})
	if err := finishTldvImport(src.Identifier, sum, err, func() error {
		return rebuildTldvCacheAfterScheduledSync(refreshCtx, "tldv:"+src.Identifier)
	}); err != nil {
		return err
	}
	return rebuildTldvCacheAfterScheduledSync(refreshCtx, "tldv:"+src.Identifier)
}

func init() {
	syncTldvCmd.Flags().IntVar(&syncTldvLimit, "limit", 0, "max meetings per run (0 = no limit)")
	syncTldvCmd.Flags().StringVar(&syncTldvAfter, "after", "", "full-sync only meetings that happened after this date (YYYY-MM-DD; implies --full)")
	syncTldvCmd.Flags().BoolVar(&syncTldvFull, "full", false, "ignore stored cursor and re-fetch every meeting (repairs existing rows in place)")
	rootCmd.AddCommand(addTldvCmd)
	rootCmd.AddCommand(syncTldvCmd)
}
