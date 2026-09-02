package store_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/msgvault/internal/store"
	"go.kenn.io/msgvault/internal/testutil"
	"go.kenn.io/msgvault/internal/testutil/storetest"
)

// pruneConversation creates a conversation and one message in it, returning
// both ids. Each message gets a body, a raw MIME blob, a recipient and an FTS
// document so the cascade and the FTS cleanup have something to remove.
func pruneConversation(
	t *testing.T, f *storetest.Fixture, convKey, title, subject string,
) (convID, msgID int64) {
	t.Helper()
	convID, err := f.Store.EnsureConversation(f.Source.ID, convKey, title)
	require.NoError(t, err, "EnsureConversation %s", convKey)
	msgID = pruneMessageIn(t, f, convID, convKey+"-msg", subject)
	return convID, msgID
}

// pruneMessageIn adds one fully populated message to an existing conversation.
func pruneMessageIn(
	t *testing.T, f *storetest.Fixture, convID int64, sourceMessageID, subject string,
) int64 {
	t.Helper()
	id, err := f.Store.UpsertMessage(&store.Message{
		ConversationID:  convID,
		SourceID:        f.Source.ID,
		SourceMessageID: sourceMessageID,
		MessageType:     "email",
		Subject:         sql.NullString{String: subject, Valid: true},
		SizeEstimate:    1000,
	})
	require.NoError(t, err, "UpsertMessage %s", sourceMessageID)

	require.NoError(t, f.Store.UpsertMessageBody(id,
		sql.NullString{String: "body of " + subject, Valid: true},
		sql.NullString{}), "UpsertMessageBody %s", sourceMessageID)
	require.NoError(t, f.Store.UpsertMessageRaw(id,
		[]byte("Subject: "+subject+"\r\n\r\nraw")), "UpsertMessageRaw %s", sourceMessageID)

	participant := f.EnsureParticipant(sourceMessageID+"@example.com", "Test User", "example.com")
	require.NoError(t, f.Store.ReplaceMessageRecipients(id, "from",
		[]int64{participant}, []string{"Test User"}), "ReplaceMessageRecipients %s", sourceMessageID)

	if f.Store.FTS5Available() {
		require.NoError(t, f.Store.UpsertFTS(id, subject, "body of "+subject,
			sourceMessageID+"@example.com", "", ""), "UpsertFTS %s", sourceMessageID)
	}
	return id
}

// pruneAuthoredMessage adds a message to convID with an explicit sender.
// A bot sender is created the way the chat importers create one — through
// EnsureParticipantByIdentifier, which stores a participant with no email
// address — so the test exercises the same rows production writes.
func pruneAuthoredMessage(
	t *testing.T, f *storetest.Fixture, convID int64, sourceMessageID string, bot bool,
) int64 {
	t.Helper()
	var (
		senderID int64
		err      error
	)
	if bot {
		senderID, err = f.Store.EnsureParticipantByIdentifier(
			"slack", "T0123:"+sourceMessageID, "Deploy Bot")
	} else {
		senderID, err = f.Store.EnsureParticipant(
			sourceMessageID+"@example.com", "Test Engineer", "example.com")
	}
	require.NoError(t, err, "create sender for %s", sourceMessageID)

	id, err := f.Store.UpsertMessage(&store.Message{
		ConversationID:  convID,
		SourceID:        f.Source.ID,
		SourceMessageID: sourceMessageID,
		MessageType:     "slack",
		SenderID:        sql.NullInt64{Int64: senderID, Valid: true},
		Subject:         sql.NullString{String: sourceMessageID, Valid: true},
		SizeEstimate:    100,
	})
	require.NoError(t, err, "UpsertMessage %s", sourceMessageID)
	return id
}

// countRows returns COUNT(*) over table filtered by a single message id.
func countRows(t *testing.T, s *store.Store, table, column string, id int64) int {
	t.Helper()
	var count int
	require.NoError(t, s.DB().QueryRow(
		s.Rebind("SELECT COUNT(*) FROM "+table+" WHERE "+column+" = ?"), id,
	).Scan(&count), "count %s", table)
	return count
}

// countFTSRows returns how many FTS documents exist for a message id.
func countFTSRows(t *testing.T, s *store.Store, id int64) int {
	t.Helper()
	var count int
	require.NoError(t, s.DB().QueryRow(
		s.Rebind("SELECT COUNT(*) FROM messages_fts WHERE rowid = ?"), id,
	).Scan(&count), "count messages_fts")
	return count
}

func messageExists(t *testing.T, s *store.Store, id int64) bool {
	t.Helper()
	return countRows(t, s, "messages", "id", id) == 1
}

func TestGlobToLikePattern(t *testing.T) {
	tests := []struct {
		name string
		glob string
		want string
	}{
		{"trailing star", "#fb2-logs-*", `#fb2-logs-%`},
		{"leading and trailing", "*logs*", `%logs%`},
		{"no wildcard", "Release notes", "Release notes"},
		{"escapes like wildcards", "50%_off", `50\%\_off`},
		{"escapes the escape", `back\slash`, `back\\slash`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, store.GlobToLikePattern(tt.glob))
		})
	}
}

func TestPruneMessages_EmptyFilterIsRejected(t *testing.T) {
	f := storetest.New(t)
	_, err := f.Store.PruneMessagesContext(t.Context(), store.PruneFilter{}, store.PruneOptions{})
	require.ErrorIs(t, err, store.ErrPruneNoSelectors)

	_, err = f.Store.CountPruneMatches(t.Context(), store.PruneFilter{})
	require.ErrorIs(t, err, store.ErrPruneNoSelectors)
}

func TestPruneMessages_ByConversationTitleGlob(t *testing.T) {
	f := storetest.New(t)
	prunedConv, prunedMsg := pruneConversation(t, f, "logs-a", "#fb2-logs-app", "log line")
	_, keptMsg := pruneConversation(t, f, "team", "#fb2-team", "hello")

	// A '%' in a kept title must stay literal: the escape in the generated
	// pattern is what stops "#fb2-logs-*" from over-matching.
	_, literalMsg := pruneConversation(t, f, "pct", "100% done", "report")

	filter := store.PruneFilter{
		TitlePatterns: []string{store.GlobToLikePattern("#fb2-logs-*")},
	}
	count, err := f.Store.CountPruneMatches(t.Context(), filter)
	require.NoError(t, err, "CountPruneMatches")
	assert.Equal(t, int64(1), count, "matched message count")

	result, err := f.Store.PruneMessagesContext(t.Context(), filter, store.PruneOptions{})
	require.NoError(t, err, "PruneMessagesContext")
	assert.Equal(t, int64(1), result.MessagesDeleted, "messages deleted")
	assert.Equal(t, int64(1), result.ConversationsDeleted, "conversations deleted")
	assert.False(t, result.Interrupted, "run should have completed")

	assert.False(t, messageExists(t, f.Store, prunedMsg), "pruned message should be gone")
	assert.True(t, messageExists(t, f.Store, keptMsg), "non-matching message should survive")
	assert.True(t, messageExists(t, f.Store, literalMsg), "literal-%% title should survive")

	assert.Equal(t, 0, countRows(t, f.Store, "conversations", "id", prunedConv),
		"emptied conversation should be gone")
}

func TestPruneMessages_CascadesChildRows(t *testing.T) {
	f := storetest.New(t)
	_, prunedMsg := pruneConversation(t, f, "logs-a", "#logs-app", "log line")
	_, keptMsg := pruneConversation(t, f, "team", "#team", "hello")

	// Sanity: the child rows exist before the prune, so their absence
	// afterwards is the cascade and not a fixture that never wrote them.
	require.Equal(t, 1, countRows(t, f.Store, "message_bodies", "message_id", prunedMsg))
	require.Equal(t, 1, countRows(t, f.Store, "message_raw", "message_id", prunedMsg))
	require.Equal(t, 1, countRows(t, f.Store, "message_recipients", "message_id", prunedMsg))

	_, err := f.Store.PruneMessagesContext(t.Context(), store.PruneFilter{
		TitlePatterns: []string{store.GlobToLikePattern("#logs-*")},
	}, store.PruneOptions{})
	require.NoError(t, err, "PruneMessagesContext")

	assert.Equal(t, 0, countRows(t, f.Store, "message_bodies", "message_id", prunedMsg),
		"message_bodies should cascade")
	assert.Equal(t, 0, countRows(t, f.Store, "message_raw", "message_id", prunedMsg),
		"message_raw should cascade")
	assert.Equal(t, 0, countRows(t, f.Store, "message_recipients", "message_id", prunedMsg),
		"message_recipients should cascade")

	assert.Equal(t, 1, countRows(t, f.Store, "message_bodies", "message_id", keptMsg),
		"kept message keeps its body")
	assert.Equal(t, 1, countRows(t, f.Store, "message_raw", "message_id", keptMsg),
		"kept message keeps its raw MIME")
}

// messages_fts is a standalone FTS5 table with no triggers, so nothing but an
// explicit delete removes a pruned message's search document.
func TestPruneMessages_RemovesFTSRows(t *testing.T) {
	testutil.SkipIfPostgres(t, "messages_fts is the SQLite FTS5 table; PostgreSQL keeps search_fts on the messages row")
	f := storetest.New(t)
	require.True(t, f.Store.FTS5Available(), "test binary must be built with FTS5 (-tags fts5)")

	_, prunedMsg := pruneConversation(t, f, "logs-a", "#logs-app", "log line")
	_, keptMsg := pruneConversation(t, f, "team", "#team", "hello")

	require.Equal(t, 1, countFTSRows(t, f.Store, prunedMsg), "FTS row before prune")
	require.Equal(t, 1, countFTSRows(t, f.Store, keptMsg), "FTS row before prune")

	_, err := f.Store.PruneMessagesContext(t.Context(), store.PruneFilter{
		TitlePatterns: []string{store.GlobToLikePattern("#logs-*")},
	}, store.PruneOptions{})
	require.NoError(t, err, "PruneMessagesContext")

	assert.Equal(t, 0, countFTSRows(t, f.Store, prunedMsg), "pruned message should lose its FTS row")
	assert.Equal(t, 1, countFTSRows(t, f.Store, keptMsg), "kept message should keep its FTS row")
}

// reply_to_message_id is a self-reference with no ON DELETE action and
// foreign keys are enforced, so an unrepaired referrer aborts the delete.
func TestPruneMessages_ClearsReplyReferences(t *testing.T) {
	f := storetest.New(t)
	prunedConv, root := pruneConversation(t, f, "logs-a", "#logs-app", "thread root")
	// A reply inside the pruned set: it must not block its root's delete.
	reply := pruneMessageIn(t, f, prunedConv, "logs-a-reply", "thread reply")
	require.NoError(t, f.Store.SetReplyTo(f.Source.ID, "logs-a-reply", "logs-a-msg"), "SetReplyTo reply")

	// A SURVIVING message pointing into the pruned set: it must end up NULL
	// rather than dangling.
	keptConv, survivor := pruneConversation(t, f, "team", "#team", "cross-thread reply")
	require.NoError(t, f.Store.SetReplyTo(f.Source.ID, "team-msg", "logs-a-msg"), "SetReplyTo survivor")
	require.NotZero(t, keptConv)

	var before sql.NullInt64
	require.NoError(t, f.Store.DB().QueryRow(
		f.Store.Rebind("SELECT reply_to_message_id FROM messages WHERE id = ?"), survivor,
	).Scan(&before), "read survivor reply link")
	require.True(t, before.Valid, "survivor must point at the root before the prune")
	require.Equal(t, root, before.Int64, "survivor reply target")

	result, err := f.Store.PruneMessagesContext(t.Context(), store.PruneFilter{
		TitlePatterns: []string{store.GlobToLikePattern("#logs-*")},
	}, store.PruneOptions{})
	require.NoError(t, err, "PruneMessagesContext")
	assert.Equal(t, int64(2), result.MessagesDeleted, "root and reply both pruned")

	assert.False(t, messageExists(t, f.Store, root), "root should be gone")
	assert.False(t, messageExists(t, f.Store, reply), "reply should be gone")

	var after sql.NullInt64
	require.NoError(t, f.Store.DB().QueryRow(
		f.Store.Rebind("SELECT reply_to_message_id FROM messages WHERE id = ?"), survivor,
	).Scan(&after), "read survivor reply link after prune")
	assert.False(t, after.Valid, "surviving referrer should be NULL, not dangling")
}

// A batch size of 1 forces one transaction per message; the loop must still
// drain the whole selection.
func TestPruneMessages_BatchSizeOne(t *testing.T) {
	f := storetest.New(t)
	convID, err := f.Store.EnsureConversation(f.Source.ID, "logs-a", "#logs-app")
	require.NoError(t, err, "EnsureConversation")

	ids := make([]int64, 0, 5)
	for i := range 5 {
		ids = append(ids, pruneMessageIn(t, f, convID,
			"logs-a-msg-"+string(rune('a'+i)), "log line"))
	}

	result, err := f.Store.PruneMessagesContext(t.Context(), store.PruneFilter{
		TitlePatterns: []string{store.GlobToLikePattern("#logs-*")},
	}, store.PruneOptions{BatchSize: 1})
	require.NoError(t, err, "PruneMessagesContext")
	assert.Equal(t, int64(5), result.MessagesDeleted, "messages deleted")
	assert.Equal(t, int64(5), result.Batches, "one batch per message")

	for _, id := range ids {
		assert.Falsef(t, messageExists(t, f.Store, id), "message %d should be gone", id)
	}
}

func TestPruneMessages_ProgressReportsEveryBatch(t *testing.T) {
	f := storetest.New(t)
	convID, err := f.Store.EnsureConversation(f.Source.ID, "logs-a", "#logs-app")
	require.NoError(t, err, "EnsureConversation")
	for i := range 3 {
		pruneMessageIn(t, f, convID, "logs-a-msg-"+string(rune('a'+i)), "log line")
	}

	var reports []store.PruneProgress
	_, err = f.Store.PruneMessagesContext(t.Context(), store.PruneFilter{
		TitlePatterns: []string{store.GlobToLikePattern("#logs-*")},
	}, store.PruneOptions{
		BatchSize: 1,
		Total:     3,
		Progress:  func(p store.PruneProgress) { reports = append(reports, p) },
	})
	require.NoError(t, err, "PruneMessagesContext")

	require.Len(t, reports, 3, "one progress report per batch")
	assert.Equal(t, int64(3), reports[2].MessagesDeleted, "final running total")
	assert.Equal(t, int64(3), reports[0].Total, "denominator echoed back")
	assert.Equal(t, 1, reports[0].BatchRows, "rows in first batch")
}

func TestPruneMessages_BySourceRemovesSourceRow(t *testing.T) {
	f := storetest.New(t)
	_, pruned := pruneConversation(t, f, "logs-a", "#logs-app", "log line")

	other, err := f.Store.GetOrCreateSource("mbox", "other@example.com")
	require.NoError(t, err, "GetOrCreateSource other")
	otherConv, err := f.Store.EnsureConversation(other.ID, "other-thread", "Other")
	require.NoError(t, err, "EnsureConversation other")
	otherMsg, err := f.Store.UpsertMessage(&store.Message{
		ConversationID:  otherConv,
		SourceID:        other.ID,
		SourceMessageID: "other-msg",
		MessageType:     "email",
		SizeEstimate:    10,
	})
	require.NoError(t, err, "UpsertMessage other")

	result, err := f.Store.PruneMessagesContext(t.Context(),
		store.PruneFilter{SourceIDs: []int64{f.Source.ID}},
		store.PruneOptions{DeleteEmptySources: true})
	require.NoError(t, err, "PruneMessagesContext")
	assert.Equal(t, int64(1), result.SourcesDeleted, "sources deleted")

	assert.False(t, messageExists(t, f.Store, pruned), "pruned message should be gone")
	assert.Equal(t, 0, countRows(t, f.Store, "sources", "id", f.Source.ID),
		"emptied source row should be gone")

	assert.True(t, messageExists(t, f.Store, otherMsg), "other source's message survives")
	assert.Equal(t, 1, countRows(t, f.Store, "sources", "id", other.ID),
		"other source row survives")
}

func TestPruneMessages_BySourceKeepSource(t *testing.T) {
	f := storetest.New(t)
	_, pruned := pruneConversation(t, f, "logs-a", "#logs-app", "log line")

	result, err := f.Store.PruneMessagesContext(t.Context(),
		store.PruneFilter{SourceIDs: []int64{f.Source.ID}},
		store.PruneOptions{DeleteEmptySources: false})
	require.NoError(t, err, "PruneMessagesContext")
	assert.Equal(t, int64(0), result.SourcesDeleted, "no source should be deleted")

	assert.False(t, messageExists(t, f.Store, pruned), "pruned message should be gone")
	assert.Equal(t, 1, countRows(t, f.Store, "sources", "id", f.Source.ID),
		"source row should be kept")
}

// A source that still holds messages is never removed, even when the caller
// asked for source cleanup — the prune only emptied part of it.
func TestDeleteSourceIfEmpty_KeepsNonEmptySource(t *testing.T) {
	f := storetest.New(t)
	_, kept := pruneConversation(t, f, "team", "#team", "hello")

	removed, err := f.Store.DeleteSourceIfEmpty(t.Context(), f.Source.ID)
	require.NoError(t, err, "DeleteSourceIfEmpty")
	assert.False(t, removed, "non-empty source must not be removed")
	assert.True(t, messageExists(t, f.Store, kept), "its message is untouched")
	assert.Equal(t, 1, countRows(t, f.Store, "sources", "id", f.Source.ID), "source row remains")
}

// CountPruneMatches is what --dry-run reports; running it must leave the
// archive exactly as it was.
func TestCountPruneMatches_ChangesNothing(t *testing.T) {
	f := storetest.New(t)
	convID, err := f.Store.EnsureConversation(f.Source.ID, "logs-a", "#logs-app")
	require.NoError(t, err, "EnsureConversation")
	ids := make([]int64, 0, 3)
	for i := range 3 {
		ids = append(ids, pruneMessageIn(t, f, convID,
			"logs-a-msg-"+string(rune('a'+i)), "log line"))
	}
	_, kept := pruneConversation(t, f, "team", "#team", "hello")

	count, err := f.Store.CountPruneMatches(t.Context(), store.PruneFilter{
		TitlePatterns: []string{store.GlobToLikePattern("#logs-*")},
	})
	require.NoError(t, err, "CountPruneMatches")
	assert.Equal(t, int64(3), count, "dry-run count")

	for _, id := range ids {
		assert.Truef(t, messageExists(t, f.Store, id), "message %d must survive a count", id)
	}
	assert.True(t, messageExists(t, f.Store, kept), "kept message must survive a count")
}

// The two selector kinds combine with OR, and a message matched by both is
// counted once in the union.
func TestCountPruneMatches_SelectorsUnion(t *testing.T) {
	f := storetest.New(t)
	pruneConversation(t, f, "logs-a", "#logs-app", "log line")
	pruneConversation(t, f, "team", "#team", "hello")

	filter := store.PruneFilter{
		SourceIDs:     []int64{f.Source.ID},
		TitlePatterns: []string{store.GlobToLikePattern("#logs-*")},
	}
	count, err := f.Store.CountPruneMatches(t.Context(), filter)
	require.NoError(t, err, "CountPruneMatches")
	// The source alone already covers both messages; the overlapping title
	// selector must not double-count.
	assert.Equal(t, int64(2), count, "union count")
}

// A log channel is a bot firehose that people nevertheless talk in.
// --bots-only must take the bot output and leave every human message —
// including the conversation row itself, which is still in use.
func TestPruneMessages_BotsOnlyKeepsHumanMessages(t *testing.T) {
	f := storetest.New(t)
	logConv, err := f.Store.EnsureConversation(f.Source.ID, "logs-a", "#fb2-logs-app")
	require.NoError(t, err, "EnsureConversation logs")

	bots := []int64{
		pruneAuthoredMessage(t, f, logConv, "logs-bot-1", true),
		pruneAuthoredMessage(t, f, logConv, "logs-bot-2", true),
		pruneAuthoredMessage(t, f, logConv, "logs-bot-3", true),
	}
	humans := []int64{
		pruneAuthoredMessage(t, f, logConv, "logs-human-1", false),
		pruneAuthoredMessage(t, f, logConv, "logs-human-2", false),
	}
	// A message with no sender at all counts as bot output: an authorless
	// row is not a person's message either.
	senderless := pruneMessageIn(t, f, logConv, "logs-senderless", "no author")

	filter := store.PruneFilter{
		TitlePatterns: []string{store.GlobToLikePattern("#fb2-logs-*")},
		BotsOnly:      true,
	}

	counts, err := f.Store.CountPruneMatchesSplit(t.Context(), filter)
	require.NoError(t, err, "CountPruneMatchesSplit")
	assert.Equal(t, int64(6), counts.Total, "whole scope, ignoring BotsOnly")
	assert.Equal(t, int64(4), counts.Bots, "three bot posts plus the senderless row")
	assert.Equal(t, int64(2), counts.Humans, "engineers' messages")

	targeted, err := f.Store.CountPruneMatches(t.Context(), filter)
	require.NoError(t, err, "CountPruneMatches")
	assert.Equal(t, int64(4), targeted, "BotsOnly narrows what will be deleted")

	result, err := f.Store.PruneMessagesContext(t.Context(), filter, store.PruneOptions{BatchSize: 2})
	require.NoError(t, err, "PruneMessagesContext")
	assert.Equal(t, int64(4), result.MessagesDeleted, "only bot messages deleted")

	for _, id := range bots {
		assert.Falsef(t, messageExists(t, f.Store, id), "bot message %d should be gone", id)
	}
	assert.False(t, messageExists(t, f.Store, senderless), "senderless message should be gone")
	for _, id := range humans {
		assert.Truef(t, messageExists(t, f.Store, id), "human message %d must survive", id)
	}

	// The conversation still holds human messages, so it must stay.
	assert.Equal(t, int64(0), result.ConversationsDeleted, "no conversation should be emptied")
	assert.Equal(t, 1, countRows(t, f.Store, "conversations", "id", logConv),
		"a conversation people still use must not be removed")
}

// Without --bots-only the same selection takes everything, humans included.
// This is the behaviour the flag exists to opt out of, so it is pinned.
func TestPruneMessages_WithoutBotsOnlyTakesHumanMessages(t *testing.T) {
	f := storetest.New(t)
	logConv, err := f.Store.EnsureConversation(f.Source.ID, "logs-a", "#fb2-logs-app")
	require.NoError(t, err, "EnsureConversation logs")
	bot := pruneAuthoredMessage(t, f, logConv, "logs-bot-1", true)
	human := pruneAuthoredMessage(t, f, logConv, "logs-human-1", false)

	result, err := f.Store.PruneMessagesContext(t.Context(), store.PruneFilter{
		TitlePatterns: []string{store.GlobToLikePattern("#fb2-logs-*")},
	}, store.PruneOptions{})
	require.NoError(t, err, "PruneMessagesContext")
	assert.Equal(t, int64(2), result.MessagesDeleted, "both messages deleted")

	assert.False(t, messageExists(t, f.Store, bot), "bot message gone")
	assert.False(t, messageExists(t, f.Store, human), "human message gone too")
	assert.Equal(t, int64(1), result.ConversationsDeleted, "now-empty conversation removed")
}

// BotsOnly narrows a selection; it cannot make one.
func TestPruneMessages_BotsOnlyIsNotASelector(t *testing.T) {
	f := storetest.New(t)
	_, err := f.Store.CountPruneMatches(t.Context(), store.PruneFilter{BotsOnly: true})
	require.ErrorIs(t, err, store.ErrPruneNoSelectors)
}

// Cancellation stops the run cleanly and reports what committed; nothing is
// rolled back, so a re-run finishes the job.
func TestPruneMessages_CancelledRunIsResumable(t *testing.T) {
	f := storetest.New(t)
	convID, err := f.Store.EnsureConversation(f.Source.ID, "logs-a", "#logs-app")
	require.NoError(t, err, "EnsureConversation")
	for i := range 4 {
		pruneMessageIn(t, f, convID, "logs-a-msg-"+string(rune('a'+i)), "log line")
	}

	filter := store.PruneFilter{TitlePatterns: []string{store.GlobToLikePattern("#logs-*")}}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result, err := f.Store.PruneMessagesContext(ctx, filter, store.PruneOptions{
		BatchSize: 1,
		// Cancel after the first batch commits.
		Progress: func(store.PruneProgress) { cancel() },
	})
	require.NoError(t, err, "a cancelled prune reports rather than errors")
	assert.True(t, result.Interrupted, "run should report itself interrupted")
	assert.Equal(t, int64(1), result.MessagesDeleted, "only the committed batch counts")

	remaining, err := f.Store.CountPruneMatches(t.Context(), filter)
	require.NoError(t, err, "CountPruneMatches after cancel")
	assert.Equal(t, int64(3), remaining, "the rest is still there")

	resumed, err := f.Store.PruneMessagesContext(t.Context(), filter, store.PruneOptions{BatchSize: 1})
	require.NoError(t, err, "resumed PruneMessagesContext")
	assert.Equal(t, int64(3), resumed.MessagesDeleted, "re-run finishes the job")
	assert.False(t, resumed.Interrupted, "re-run completes")
}
