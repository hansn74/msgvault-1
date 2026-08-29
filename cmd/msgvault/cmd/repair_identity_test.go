package cmd

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	imapclient "go.kenn.io/msgvault/internal/imap"
	"go.kenn.io/msgvault/internal/testutil"
)

// repair-identity must confirm the own-address identity for exactly the
// email-shaped, identity-less sources of the requested type, and be idempotent.
func TestRepairIdentities_Selection(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)
	st := testutil.NewTestStore(t)

	a, err := st.GetOrCreateSource("gmail", "frank@example.com") // email, no identity -> repaired
	require.NoError(err)
	b, err := st.GetOrCreateSource("gmail", "alice@example.com") // already confirmed -> skipped
	require.NoError(err)
	require.NoError(st.AddAccountIdentity(b.ID, "alice@example.com", "preexisting"))
	c, err := st.GetOrCreateSource("gmail", "notanemail") // non-email identifier -> skipped
	require.NoError(err)
	d, err := st.GetOrCreateSource("imap", "bob@example.com") // wrong type -> skipped
	require.NoError(err)
	e, err := st.GetOrCreateSource("gmail", "https://user@example.com/mail") // URL, not an email -> skipped
	require.NoError(err)

	var buf bytes.Buffer
	n, err := repairIdentities(st, "gmail", "", &buf)
	require.NoError(err)
	assert.Equal(1, n, "only the valid gmail address without an identity is repaired")

	ai, err := st.ListAccountIdentities(a.ID)
	require.NoError(err)
	assert.Len(ai, 1, "frank gets his own address confirmed")

	ci, err := st.ListAccountIdentities(c.ID)
	require.NoError(err)
	assert.Empty(ci, "non-email identifier is left alone")

	di, err := st.ListAccountIdentities(d.ID)
	require.NoError(err)
	assert.Empty(di, "imap source untouched when repairing gmail")

	ei, err := st.ListAccountIdentities(e.ID)
	require.NoError(err)
	assert.Empty(ei, "URL identifier is not accepted as an email address")

	bi, err := st.ListAccountIdentities(b.ID)
	require.NoError(err)
	assert.Len(bi, 1, "already-confirmed source is not doubled")

	// Idempotent: a second run repairs nothing (all now confirmed).
	buf.Reset()
	n2, err := repairIdentities(st, "gmail", "", &buf)
	require.NoError(err)
	assert.Zero(n2, "re-running repairs nothing once identities are confirmed")
}

// The onlyIdentifier argument scopes the repair to a single account.
func TestRepairIdentities_SingleTarget(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)
	st := testutil.NewTestStore(t)
	a, err := st.GetOrCreateSource("gmail", "frank@example.com")
	require.NoError(err)
	_, err = st.GetOrCreateSource("gmail", "carol@example.com")
	require.NoError(err)

	var buf bytes.Buffer
	n, err := repairIdentities(st, "gmail", "FRANK@example.com", &buf) // case-insensitive
	require.NoError(err)
	assert.Equal(1, n)

	ai, err := st.ListAccountIdentities(a.ID)
	require.NoError(err)
	assert.Len(ai, 1, "only the targeted account is confirmed")
}

func TestRepairIdentities_IMAPUsesConfiguredUsername(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)
	st := testutil.NewTestStore(t)
	imapCfg := &imapclient.Config{
		Host: "mail.example.com", Port: 993, TLS: true, Username: "owner@example.com",
	}
	source, err := st.GetOrCreateSource("imap", imapCfg.Identifier())
	require.NoError(err)
	configJSON, err := imapCfg.ToJSON()
	require.NoError(err)
	require.NoError(st.UpdateSourceSyncConfig(source.ID, configJSON))

	var buf bytes.Buffer
	repaired, err := repairIdentities(st, "imap", "", &buf)
	require.NoError(err)
	assert.Equal(1, repaired)

	identities, err := st.ListAccountIdentities(source.ID)
	require.NoError(err)
	require.Len(identities, 1)
	assert.Equal("owner@example.com", identities[0].Address)
}

func TestRepairIdentities_SkipsOnlyMatchingIdentity(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)
	st := testutil.NewTestStore(t)

	aliasOnly, err := st.GetOrCreateSource("gmail", "owner@example.com")
	require.NoError(err)
	require.NoError(st.AddAccountIdentity(aliasOnly.ID, "alias@example.com", "preexisting"))
	matching, err := st.GetOrCreateSource("gmail", "confirmed@example.com")
	require.NoError(err)
	require.NoError(st.AddAccountIdentity(matching.ID, "CONFIRMED@EXAMPLE.COM", "preexisting"))

	var buf bytes.Buffer
	repaired, err := repairIdentities(st, "gmail", "", &buf)
	require.NoError(err)
	assert.Equal(1, repaired, "only the source missing its own address is repaired")

	aliasIdentities, err := st.ListAccountIdentities(aliasOnly.ID)
	require.NoError(err)
	assert.Len(aliasIdentities, 2, "an unrelated alias must not suppress the account address")
	matchingIdentities, err := st.ListAccountIdentities(matching.ID)
	require.NoError(err)
	assert.Len(matchingIdentities, 1, "case-equivalent account address is already confirmed")
}

func TestRepairIdentities_ReturnsAddFailuresAfterRemainingSources(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)
	st := testutil.NewTestStore(t)

	failed, err := st.GetOrCreateSource("gmail", "fail@example.com")
	require.NoError(err)
	succeeded, err := st.GetOrCreateSource("gmail", "succeed@example.com")
	require.NoError(err)
	_, err = st.DB().Exec(`
		CREATE TRIGGER fail_repair_identity
		BEFORE INSERT ON account_identities
		WHEN NEW.address = 'fail@example.com'
		BEGIN
			SELECT RAISE(FAIL, 'forced identity failure');
		END`)
	require.NoError(err)

	var buf bytes.Buffer
	repaired, err := repairIdentities(st, "gmail", "", &buf)
	require.Error(err)
	require.ErrorContains(err, "fail@example.com")
	require.ErrorContains(err, "forced identity failure")
	assert.Equal(1, repaired, "the later source is still repaired")

	failedIdentities, listErr := st.ListAccountIdentities(failed.ID)
	require.NoError(listErr)
	assert.Empty(failedIdentities)
	succeededIdentities, listErr := st.ListAccountIdentities(succeeded.ID)
	require.NoError(listErr)
	assert.Len(succeededIdentities, 1)
}
