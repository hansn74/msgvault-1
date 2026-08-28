package cmd

import (
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"
	"go.kenn.io/msgvault/internal/store"
)

var (
	repairIdentityType string
)

func newRepairIdentityCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "repair-identity [identifier]",
		Short: "Confirm account identities and recompute is_from_me on existing messages",
		Long: `Confirm each account's own address as its identity and recompute the
is_from_me attribution on messages already in the archive.

Provider syncs (e.g. Gmail via a service account) can create a source without
confirming its own address as an account identity. When that happens, no
message is attributed to the account owner: is_from_me stays 0 for everything,
so "what did this person send" cannot be answered. This command adds the
account's own address as a confirmed identity, which in the same transaction
retro-attributes matching earlier messages (those the account sent) as
is_from_me = 1.

It is idempotent and safe: sources that already have a confirmed identity are
skipped (re-adding an existing identity would not re-attribute), and only
sources whose identifier looks like an email address are touched.

Examples:
  msgvault repair-identity                       # all Gmail accounts missing an identity
  msgvault repair-identity you@example.com       # one account
  msgvault repair-identity --type imap`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if !isDaemonCLISubprocess() {
				return runDaemonCLICommandHTTPFromCobra(cmd, args)
			}
			s, cleanup, err := openWritableStoreAndInitForIngest()
			if err != nil {
				return err
			}
			defer cleanup()

			only := ""
			if len(args) == 1 {
				only = strings.TrimSpace(args[0])
			}
			repaired, rerr := repairIdentities(s, repairIdentityType, only, cmd.OutOrStdout())
			cacheErr := rebuildCacheAfterWrite(cfg.DatabaseDSN())
			if rerr != nil {
				return errors.Join(rerr, cacheErr)
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Repaired %d source(s).\n", repaired)
			return cacheErr
		},
	}
	cmd.Flags().StringVar(&repairIdentityType, "type", "gmail", "source type to repair")
	return cmd
}

// repairIdentities confirms the own-address identity (and retro-attributes
// is_from_me) for every source of sourceType whose identifier is an email and
// which has no confirmed identity yet. When onlyIdentifier is non-empty, only
// that account is considered. Returns the number of sources repaired.
func repairIdentities(s *store.Store, sourceType, onlyIdentifier string, out io.Writer) (int, error) {
	sources, err := s.ListSources(sourceType)
	if err != nil {
		return 0, fmt.Errorf("list %s sources: %w", sourceType, err)
	}
	repaired := 0
	for _, src := range sources {
		id := strings.TrimSpace(src.Identifier)
		if onlyIdentifier != "" && !strings.EqualFold(id, onlyIdentifier) {
			continue
		}
		if !strings.Contains(id, "@") {
			continue // not an email-shaped identifier; nothing to attribute against
		}
		existing, lerr := s.ListAccountIdentities(src.ID)
		if lerr != nil {
			return repaired, fmt.Errorf("list identities for %s: %w", id, lerr)
		}
		if len(existing) > 0 {
			continue // already confirmed; AddAccountIdentity would not re-attribute
		}
		before := countFromMe(s, src.ID)
		// AddAccountIdentity's insert hook retro-attributes matching messages in
		// the same transaction (see store.addAccountIdentityContext).
		if err := s.AddAccountIdentity(src.ID, id, "identity-repair"); err != nil {
			_, _ = fmt.Fprintf(out, "  %s: FAILED: %v\n", id, err)
			continue
		}
		after := countFromMe(s, src.ID)
		_, _ = fmt.Fprintf(out, "  %s: confirmed identity; is_from_me %d -> %d\n", id, before, after)
		repaired++
	}
	return repaired, nil
}

// countFromMe returns how many messages in a source are attributed to the
// account owner. Best-effort: a query error yields -1 (shown but non-fatal).
func countFromMe(s *store.Store, sourceID int64) int {
	var n int
	if err := s.DB().QueryRow(
		s.Rebind(`SELECT COUNT(*) FROM messages WHERE source_id = ? AND is_from_me = 1`),
		sourceID).Scan(&n); err != nil {
		return -1
	}
	return n
}

func init() {
	rootCmd.AddCommand(newRepairIdentityCmd())
}
