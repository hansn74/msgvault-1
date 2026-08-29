package cmd

import (
	"errors"
	"fmt"
	"io"
	"net/mail"
	"strings"

	"github.com/spf13/cobra"
	imapclient "go.kenn.io/msgvault/internal/imap"
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

It is idempotent and safe: a source is skipped when its account address is
already confirmed, and only plain, valid email addresses are touched. Other
confirmed aliases do not suppress repair of the account address.

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
// is_from_me) for every source of sourceType whose resolved account address is
// a valid email and is not already confirmed. When onlyIdentifier is non-empty,
// only a matching source identifier or resolved account address is considered.
// Returns the number of sources repaired and any identity-add failures after
// attempting every candidate.
func repairIdentities(s *store.Store, sourceType, onlyIdentifier string, out io.Writer) (int, error) {
	sources, err := s.ListSources(sourceType)
	if err != nil {
		return 0, fmt.Errorf("list %s sources: %w", sourceType, err)
	}
	repaired := 0
	var failures []error
	for _, src := range sources {
		sourceIdentifier := strings.TrimSpace(src.Identifier)
		address := repairIdentityAddress(src)
		if onlyIdentifier != "" &&
			!strings.EqualFold(sourceIdentifier, onlyIdentifier) &&
			!store.EqualIdentifier(address, onlyIdentifier) {
			continue
		}
		parsed, parseErr := mail.ParseAddress(address)
		if parseErr != nil || parsed.Name != "" || !strings.EqualFold(parsed.Address, address) {
			continue // not one plain email address; nothing to attribute against
		}
		existing, lerr := s.ListAccountIdentities(src.ID)
		if lerr != nil {
			return repaired, fmt.Errorf("list identities for %s: %w", sourceIdentifier, lerr)
		}
		alreadyConfirmed := false
		for _, identity := range existing {
			if store.EqualIdentifier(identity.Address, address) {
				alreadyConfirmed = true
				break
			}
		}
		if alreadyConfirmed {
			continue // this account address is already confirmed
		}
		before := countFromMe(s, src.ID)
		// AddAccountIdentity's insert hook retro-attributes matching messages in
		// the same transaction (see store.addAccountIdentityContext).
		if err := s.AddAccountIdentity(src.ID, address, "identity-repair"); err != nil {
			_, _ = fmt.Fprintf(out, "  %s: FAILED: %v\n", sourceIdentifier, err)
			failures = append(failures, fmt.Errorf("add identity for %s: %w", sourceIdentifier, err))
			continue
		}
		after := countFromMe(s, src.ID)
		_, _ = fmt.Fprintf(out, "  %s: confirmed identity %s; is_from_me %d -> %d\n",
			sourceIdentifier, address, before, after)
		repaired++
	}
	return repaired, errors.Join(failures...)
}

func repairIdentityAddress(src *store.Source) string {
	if src.SourceType != sourceTypeIMAP {
		return strings.TrimSpace(src.Identifier)
	}
	if src.SyncConfig.Valid {
		imapCfg, err := imapclient.ConfigFromJSON(src.SyncConfig.String)
		if err == nil && strings.TrimSpace(imapCfg.Username) != "" {
			return strings.TrimSpace(imapCfg.Username)
		}
	}
	if src.DisplayName.Valid {
		return strings.TrimSpace(src.DisplayName.String)
	}
	return ""
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
