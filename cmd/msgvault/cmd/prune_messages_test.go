package cmd

import (
	"bytes"
	"database/sql"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/msgvault/internal/store"
	"go.kenn.io/msgvault/internal/testutil/storetest"
)

// newPruneTestMessage adds one message to a conversation created for title.
func newPruneTestMessage(
	t *testing.T, f *storetest.Fixture, convKey, title, sourceMessageID string,
) int64 {
	t.Helper()
	convID, err := f.Store.EnsureConversation(f.Source.ID, convKey, title)
	require.NoError(t, err, "EnsureConversation %s", convKey)
	id, err := f.Store.UpsertMessage(&store.Message{
		ConversationID:  convID,
		SourceID:        f.Source.ID,
		SourceMessageID: sourceMessageID,
		MessageType:     "email",
		Subject:         sql.NullString{String: title, Valid: true},
		SizeEstimate:    100,
	})
	require.NoError(t, err, "UpsertMessage %s", sourceMessageID)
	return id
}

func TestSplitPruneSourceSpec(t *testing.T) {
	tests := []struct {
		name           string
		spec           string
		wantType       string
		wantIdentifier string
		wantErr        bool
	}{
		{name: "gmail", spec: "gmail:you@example.com", wantType: "gmail", wantIdentifier: "you@example.com"},
		// Slack identifiers embed a colon; only the first one separates.
		{name: "slack", spec: "slack:T0123:U0456", wantType: "slack", wantIdentifier: "T0123:U0456"},
		{name: "no colon", spec: "you@example.com", wantErr: true},
		{name: "empty type", spec: ":you@example.com", wantErr: true},
		{name: "empty identifier", spec: "gmail:", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sourceType, identifier, err := splitPruneSourceSpec(tt.spec)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantType, sourceType, "source type")
			assert.Equal(t, tt.wantIdentifier, identifier, "identifier")
		})
	}
}

// The flags are parsed off the real command, so a rename or a changed
// default breaks this rather than passing silently.
func TestPruneMessagesFlags_Defaults(t *testing.T) {
	cmd := newPruneMessagesCmd()
	require.NoError(t, cmd.ParseFlags([]string{
		"--source", "gmail:you@example.com",
		"--conversation-title", "#logs-*",
	}))

	opts, err := pruneMessagesFlags(cmd)
	require.NoError(t, err, "pruneMessagesFlags")
	assert.Equal(t, []string{"gmail:you@example.com"}, opts.sources, "sources")
	assert.Equal(t, []string{"#logs-*"}, opts.titleGlobs, "title globs")
	assert.Equal(t, store.DefaultPruneBatchSize, opts.batchSize, "default batch size")
	assert.False(t, opts.dryRun, "dry-run defaults off")
	assert.False(t, opts.yes, "yes defaults off")
	assert.False(t, opts.keepSource, "keep-source defaults off")
	assert.False(t, opts.botsOnly, "bots-only defaults off")
	assert.True(t, opts.deleteSources, "source rows are removed unless --keep-source")
}

func TestPruneMessagesFlags_BotsOnly(t *testing.T) {
	cmd := newPruneMessagesCmd()
	require.NoError(t, cmd.ParseFlags([]string{
		"--conversation-title", "#logs-*", "--bots-only",
	}))

	opts, err := pruneMessagesFlags(cmd)
	require.NoError(t, err, "pruneMessagesFlags")
	assert.True(t, opts.botsOnly, "bots-only")
}

func TestPruneMessagesFlags_KeepSource(t *testing.T) {
	cmd := newPruneMessagesCmd()
	require.NoError(t, cmd.ParseFlags([]string{
		"--source", "gmail:you@example.com", "--keep-source", "--batch-size", "50", "--dry-run", "-y",
	}))

	opts, err := pruneMessagesFlags(cmd)
	require.NoError(t, err, "pruneMessagesFlags")
	assert.True(t, opts.keepSource, "keep-source")
	assert.False(t, opts.deleteSources, "--keep-source must suppress source removal")
	assert.Equal(t, 50, opts.batchSize, "batch size")
	assert.True(t, opts.dryRun, "dry-run")
	assert.True(t, opts.yes, "yes")
}

func TestValidatePruneSelectors(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{name: "no selector", args: nil, wantErr: "at least one --source"},
		{name: "bad source spec", args: []string{"--source", "no-colon"}, wantErr: "expected <type:identifier>"},
		{name: "non-positive batch", args: []string{"--conversation-title", "x*", "--batch-size", "0"}, wantErr: "--batch-size must be positive"},
		{name: "title only", args: []string{"--conversation-title", "#logs-*"}},
		{name: "source only", args: []string{"--source", "gmail:you@example.com"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := newPruneMessagesCmd()
			require.NoError(t, cmd.ParseFlags(tt.args))
			opts, err := pruneMessagesFlags(cmd)
			require.NoError(t, err, "pruneMessagesFlags")

			err = validatePruneSelectors(cmd, opts)
			if tt.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestResolvePrunePlan_PerSelectorCounts(t *testing.T) {
	f := storetest.New(t)
	newPruneTestMessage(t, f, "logs-a", "#fb2-logs-app", "logs-a-1")
	newPruneTestMessage(t, f, "logs-b", "#fb2-logs-api", "logs-b-1")
	newPruneTestMessage(t, f, "team", "#fb2-team", "team-1")

	other, err := f.Store.GetOrCreateSource("mbox", "other@example.com")
	require.NoError(t, err, "GetOrCreateSource")
	otherConv, err := f.Store.EnsureConversation(other.ID, "other", "Other")
	require.NoError(t, err, "EnsureConversation other")
	_, err = f.Store.UpsertMessage(&store.Message{
		ConversationID:  otherConv,
		SourceID:        other.ID,
		SourceMessageID: "other-1",
		MessageType:     "email",
		SizeEstimate:    10,
	})
	require.NoError(t, err, "UpsertMessage other")

	plan, err := resolvePrunePlan(t.Context(), f.Store, pruneMessagesOptions{
		sources:    []string{"mbox:other@example.com"},
		titleGlobs: []string{"#fb2-logs-*"},
	})
	require.NoError(t, err, "resolvePrunePlan")

	require.Len(t, plan.Entries, 2, "one entry per selector")
	assert.Equal(t, "source mbox:other@example.com", plan.Entries[0].Label, "source label")
	assert.Equal(t, int64(1), plan.Entries[0].Counts.Total, "source match count")
	assert.Equal(t, "conversation title #fb2-logs-*", plan.Entries[1].Label, "title label")
	assert.Equal(t, int64(2), plan.Entries[1].Counts.Total, "title match count")
	assert.Equal(t, int64(3), plan.Scope.Total, "union scope")
	assert.Equal(t, int64(3), plan.Total, "messages that will be deleted")
	assert.False(t, plan.BotsOnly, "bots-only not requested")

	assert.Equal(t, []int64{other.ID}, plan.Filter.SourceIDs, "resolved source ids")
	assert.Equal(t, []string{`#fb2-logs-%`}, plan.Filter.TitlePatterns, "translated glob")
	assert.False(t, plan.Filter.BotsOnly, "filter carries the flag through")
}

// The plan must report the authorship split per selector, and --bots-only
// must move the delete total down to the bot count while the scope numbers
// still show what would be lost without the flag.
func TestResolvePrunePlan_BotsOnlySplit(t *testing.T) {
	f := storetest.New(t)
	convID, err := f.Store.EnsureConversation(f.Source.ID, "logs-a", "#fb2-logs-app")
	require.NoError(t, err, "EnsureConversation")

	botSender, err := f.Store.EnsureParticipantByIdentifier("slack", "T0123:B1", "Deploy Bot")
	require.NoError(t, err, "bot participant")
	humanSender, err := f.Store.EnsureParticipant("dev@example.com", "Dev", "example.com")
	require.NoError(t, err, "human participant")

	for i, sender := range []int64{botSender, botSender, botSender, humanSender} {
		_, err := f.Store.UpsertMessage(&store.Message{
			ConversationID:  convID,
			SourceID:        f.Source.ID,
			SourceMessageID: "logs-" + string(rune('a'+i)),
			MessageType:     "slack",
			SenderID:        sql.NullInt64{Int64: sender, Valid: true},
			SizeEstimate:    10,
		})
		require.NoError(t, err, "UpsertMessage")
	}

	base := pruneMessagesOptions{titleGlobs: []string{"#fb2-logs-*"}}

	plan, err := resolvePrunePlan(t.Context(), f.Store, base)
	require.NoError(t, err, "resolvePrunePlan without --bots-only")
	require.Len(t, plan.Entries, 1)
	assert.Equal(t, int64(4), plan.Entries[0].Counts.Total, "selector scope")
	assert.Equal(t, int64(3), plan.Entries[0].Counts.Bots, "bot messages")
	assert.Equal(t, int64(1), plan.Entries[0].Counts.Humans, "human messages")
	assert.Equal(t, int64(4), plan.Total, "without the flag, everything goes")

	botsOnly := base
	botsOnly.botsOnly = true
	plan, err = resolvePrunePlan(t.Context(), f.Store, botsOnly)
	require.NoError(t, err, "resolvePrunePlan with --bots-only")
	assert.True(t, plan.BotsOnly, "plan records the flag")
	assert.True(t, plan.Filter.BotsOnly, "filter records the flag")
	assert.Equal(t, int64(3), plan.Total, "only bot messages are deleted")
	assert.Equal(t, int64(4), plan.Scope.Total, "scope is still reported in full")
	assert.Equal(t, int64(1), plan.Scope.Humans, "humans that will be kept")
}

func TestResolvePrunePlan_UnknownSource(t *testing.T) {
	f := storetest.New(t)

	_, err := resolvePrunePlan(t.Context(), f.Store, pruneMessagesOptions{
		sources: []string{"gmail:nobody@example.com"},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nobody@example.com")
}

// The identifier exists but under a different source type, so a
// type-qualified selector must refuse rather than guess.
func TestResolvePrunePlan_WrongSourceType(t *testing.T) {
	f := storetest.New(t) // f.Source is gmail:test@example.com

	_, err := resolvePrunePlan(t.Context(), f.Store, pruneMessagesOptions{
		sources: []string{"mbox:" + f.Source.Identifier},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "gmail", "error should name the type that does exist")
}

func TestPrintPrunePlan_WarnsAboutHumanMessages(t *testing.T) {
	var buf bytes.Buffer
	printPrunePlan(&buf, prunePlan{
		Entries: []prunePlanEntry{
			{Label: "conversation title #logs-*",
				Counts: store.PruneMatchCounts{Total: 11100000, Bots: 11097193, Humans: 2807}},
		},
		Scope: store.PruneMatchCounts{Total: 11100000, Bots: 11097193, Humans: 2807},
		Total: 11100000,
	})

	out := buf.String()
	assert.Contains(t, out, "conversation title #logs-*")
	assert.Contains(t, out, "11,100,000")
	assert.Contains(t, out, "bots 11,097,193 / humans 2,807")
	assert.Contains(t, out, "TOTAL (distinct messages)")
	assert.Contains(t, out, "WARNING", "omitting --bots-only must be called out")
	assert.Contains(t, out, "2,807 human-authored message(s)")
	assert.Contains(t, out, "--bots-only")
}

func TestPrintPrunePlan_BotsOnlyReportsWhatSurvives(t *testing.T) {
	var buf bytes.Buffer
	printPrunePlan(&buf, prunePlan{
		Entries: []prunePlanEntry{
			{Label: "conversation title #logs-*",
				Counts: store.PruneMatchCounts{Total: 11100000, Bots: 11097193, Humans: 2807}},
		},
		Scope:    store.PruneMatchCounts{Total: 11100000, Bots: 11097193, Humans: 2807},
		Total:    11097193,
		BotsOnly: true,
	})

	out := buf.String()
	assert.Contains(t, out, "will delete 11,097,193 bot message(s)")
	assert.Contains(t, out, "2,807 human-authored message(s) are kept")
	assert.NotContains(t, out, "WARNING", "nothing to warn about when humans are spared")
}

func TestConfirmPruneMessages(t *testing.T) {
	opts := pruneMessagesOptions{
		sources:    []string{"gmail:you@example.com"},
		titleGlobs: []string{"#logs-*"},
	}

	t.Run("accepts yes", func(t *testing.T) {
		var out bytes.Buffer
		ok, err := confirmPruneMessages(strings.NewReader("y\n"), &out, opts)
		require.NoError(t, err)
		assert.True(t, ok, "y should confirm")
		assert.Contains(t, out.String(), "gmail:you@example.com", "prompt names the selectors")
		assert.Contains(t, out.String(), "#logs-*", "prompt names the selectors")
		assert.NotContains(t, out.String(), "--bots-only", "flag was not set")
	})

	t.Run("names the bots-only restriction", func(t *testing.T) {
		botsOnly := opts
		botsOnly.botsOnly = true
		var out bytes.Buffer
		ok, err := confirmPruneMessages(strings.NewReader("y\n"), &out, botsOnly)
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Contains(t, out.String(), "--bots-only", "prompt must name the restriction")
	})

	t.Run("declines anything else", func(t *testing.T) {
		var out bytes.Buffer
		ok, err := confirmPruneMessages(strings.NewReader("maybe\n"), &out, opts)
		require.NoError(t, err)
		assert.False(t, ok, "a non-yes answer must abort")
		assert.Contains(t, out.String(), "Aborted.")
	})

	t.Run("closed stdin is an error, not a yes", func(t *testing.T) {
		var out bytes.Buffer
		ok, err := confirmPruneMessages(strings.NewReader(""), &out, opts)
		require.Error(t, err, "EOF must not be read as consent")
		assert.False(t, ok)
	})
}

func TestPrintPruneResult_Interrupted(t *testing.T) {
	var buf bytes.Buffer
	printPruneResult(&buf, store.PruneResult{
		MessagesDeleted:      4000,
		ConversationsDeleted: 12,
		SourcesDeleted:       1,
		Batches:              2,
		Interrupted:          true,
	})

	out := buf.String()
	assert.Contains(t, out, "4,000 message(s)")
	assert.Contains(t, out, "12 now-empty conversation(s)")
	assert.Contains(t, out, "1 emptied source(s)")
	assert.Contains(t, out, "re-run the same command")
	assert.Contains(t, out, "Attachment files on disk were not deleted.")
}
