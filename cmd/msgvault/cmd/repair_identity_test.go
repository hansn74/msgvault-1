package cmd

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/msgvault/internal/testutil"
)

// repair-identity must confirm the own-address identity for exactly the
// email-shaped, identity-less sources of the requested type, and be idempotent.
func TestRepairIdentities_Selection(t *testing.T) {
	st := testutil.NewTestStore(t)

	a, err := st.GetOrCreateSource("gmail", "frank@example.com") // email, no identity -> repaired
	require.NoError(t, err)
	b, err := st.GetOrCreateSource("gmail", "alice@example.com") // already confirmed -> skipped
	require.NoError(t, err)
	require.NoError(t, st.AddAccountIdentity(b.ID, "alice@example.com", "preexisting"))
	c, err := st.GetOrCreateSource("gmail", "notanemail") // non-email identifier -> skipped
	require.NoError(t, err)
	d, err := st.GetOrCreateSource("imap", "bob@example.com") // wrong type -> skipped
	require.NoError(t, err)

	var buf bytes.Buffer
	n, err := repairIdentities(st, "gmail", "", &buf)
	require.NoError(t, err)
	assert.Equal(t, 1, n, "only the email-shaped gmail source without an identity is repaired")

	ai, err := st.ListAccountIdentities(a.ID)
	require.NoError(t, err)
	assert.Len(t, ai, 1, "frank gets his own address confirmed")

	ci, err := st.ListAccountIdentities(c.ID)
	require.NoError(t, err)
	assert.Empty(t, ci, "non-email identifier is left alone")

	di, err := st.ListAccountIdentities(d.ID)
	require.NoError(t, err)
	assert.Empty(t, di, "imap source untouched when repairing gmail")

	bi, err := st.ListAccountIdentities(b.ID)
	require.NoError(t, err)
	assert.Len(t, bi, 1, "already-confirmed source is not doubled")

	// Idempotent: a second run repairs nothing (all now confirmed).
	buf.Reset()
	n2, err := repairIdentities(st, "gmail", "", &buf)
	require.NoError(t, err)
	assert.Zero(t, n2, "re-running repairs nothing once identities are confirmed")
}

// The onlyIdentifier argument scopes the repair to a single account.
func TestRepairIdentities_SingleTarget(t *testing.T) {
	st := testutil.NewTestStore(t)
	a, err := st.GetOrCreateSource("gmail", "frank@example.com")
	require.NoError(t, err)
	_, err = st.GetOrCreateSource("gmail", "carol@example.com")
	require.NoError(t, err)

	var buf bytes.Buffer
	n, err := repairIdentities(st, "gmail", "FRANK@example.com", &buf) // case-insensitive
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	ai, err := st.ListAccountIdentities(a.ID)
	require.NoError(t, err)
	assert.Len(t, ai, 1, "only the targeted account is confirmed")
}
