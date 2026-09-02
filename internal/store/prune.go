package store

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
)

// ErrPruneNoSelectors is returned when a PruneFilter selects nothing. A prune
// with no selector would match every message in the archive, so an empty
// filter is rejected rather than interpreted.
var ErrPruneNoSelectors = errors.New("prune: no selectors given")

const (
	// DefaultPruneBatchSize is the number of messages deleted per committed
	// transaction when PruneOptions.BatchSize is unset.
	DefaultPruneBatchSize = 2000

	// MaxPruneBatchSize caps the batch size. Every id in a batch becomes a
	// bound parameter in three statements, and both backends have a hard
	// per-statement parameter ceiling (SQLite's SQLITE_MAX_VARIABLE_NUMBER,
	// PostgreSQL's 65535). This leaves ample headroom under both.
	MaxPruneBatchSize = 5000

	// DefaultPruneCheckpointEvery is how many committed batches pass between
	// passive WAL checkpoints when PruneOptions.CheckpointEvery is unset.
	DefaultPruneCheckpointEvery = 20
)

// pruneLikeEscape is the ESCAPE clause paired with every pattern produced by
// GlobToLikePattern. Both backends accept it and read '\' literally (the
// server's standard_conforming_strings default on PostgreSQL).
const pruneLikeEscape = ` ESCAPE '\'`

// PruneFilter selects the messages a prune run targets. The two SELECTORS
// are combined with OR: a message is in scope if its source is listed OR its
// conversation title matches one of the patterns. BotsOnly is not a selector
// but a FILTER, and ANDs with that scope. A filter with no selector is
// invalid — BotsOnly alone would target every bot message in the archive.
type PruneFilter struct {
	// SourceIDs targets every message belonging to these sources.
	SourceIDs []int64

	// TitlePatterns targets every message whose conversation title matches
	// one of these SQL LIKE patterns. Build them with GlobToLikePattern
	// rather than by hand — the queries bind them with an ESCAPE clause.
	TitlePatterns []string

	// BotsOnly narrows the selection to messages with no human author, so a
	// prune aimed at a bot-firehose channel does not take the handful of
	// engineer messages sitting in it. See pruneBotSenderExpr for what
	// counts as "no human author" and why.
	BotsOnly bool
}

// IsEmpty reports whether the filter selects nothing. BotsOnly is not
// counted: it narrows a selection rather than making one.
func (f PruneFilter) IsEmpty() bool {
	return len(f.SourceIDs) == 0 && len(f.TitlePatterns) == 0
}

// pruneBotSenderExpr is the SQL predicate for "this message has no human
// author", with alias naming the messages table.
//
// The discriminator is the sender's profile email address. Every human
// participant carries one; a bot, webhook, or integration post resolves to a
// participant that has none, because the platform never publishes an address
// for it. A message with no sender at all is treated the same way — an
// authorless row is not a person's message either.
//
// This is deliberately conservative in the direction that matters: a message
// whose sender has SOME email address is never treated as a bot, so an
// ambiguous row survives the prune rather than being destroyed by it.
func pruneBotSenderExpr(alias string) string {
	prefix := ""
	if alias != "" {
		prefix = alias + "."
	}
	return "(" + prefix + "sender_id IS NULL OR EXISTS (" +
		"SELECT 1 FROM participants p WHERE p.id = " + prefix + "sender_id" +
		" AND (p.email_address IS NULL OR p.email_address = '')))"
}

// GlobToLikePattern translates a shell-style glob — where '*' stands for any
// run of characters — into a SQL LIKE pattern. LIKE's own wildcards ('%' and
// '_') and the escape character itself are escaped, so a title containing
// them matches literally instead of silently widening the selection. Pair the
// result with pruneLikeEscape, which every query in this file does.
func GlobToLikePattern(glob string) string {
	var b strings.Builder
	b.Grow(len(glob) + 4)
	for _, r := range glob {
		switch r {
		case '*':
			b.WriteRune('%')
		case '%', '_', '\\':
			b.WriteRune('\\')
			b.WriteRune(r)
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

// messagePredicate builds the WHERE fragment matching the targeted messages,
// with alias naming the messages table in the enclosing statement.
//
// The title arm uses EXISTS rather than a join so the predicate stays a
// semi-join and cannot duplicate a message row.
func (f PruneFilter) messagePredicate(alias string) (string, []any, error) {
	if f.IsEmpty() {
		return "", nil, ErrPruneNoSelectors
	}
	prefix := ""
	if alias != "" {
		prefix = alias + "."
	}
	var (
		clauses []string
		args    []any
	)
	if len(f.SourceIDs) > 0 {
		clauses = append(clauses, prefix+"source_id IN ("+placeholderList(len(f.SourceIDs))+")")
		for _, id := range f.SourceIDs {
			args = append(args, id)
		}
	}
	if len(f.TitlePatterns) > 0 {
		likes := make([]string, len(f.TitlePatterns))
		for i, pattern := range f.TitlePatterns {
			likes[i] = "c.title LIKE ?" + pruneLikeEscape
			args = append(args, pattern)
		}
		clauses = append(clauses,
			"EXISTS (SELECT 1 FROM conversations c WHERE c.id = "+prefix+
				"conversation_id AND ("+strings.Join(likes, " OR ")+"))")
	}
	where := "(" + strings.Join(clauses, " OR ") + ")"
	if f.BotsOnly {
		where += " AND " + pruneBotSenderExpr(alias)
	}
	return where, args, nil
}

// conversationPredicate builds the WHERE fragment matching the conversations
// in the prune's scope. It is deliberately scope-limited rather than "every
// empty conversation in the archive": an archive can hold conversations that
// were already empty for unrelated reasons, and a prune has no mandate over
// those.
func (f PruneFilter) conversationPredicate() (string, []any, error) {
	if f.IsEmpty() {
		return "", nil, ErrPruneNoSelectors
	}
	var (
		clauses []string
		args    []any
	)
	if len(f.SourceIDs) > 0 {
		clauses = append(clauses, "source_id IN ("+placeholderList(len(f.SourceIDs))+")")
		for _, id := range f.SourceIDs {
			args = append(args, id)
		}
	}
	if len(f.TitlePatterns) > 0 {
		likes := make([]string, len(f.TitlePatterns))
		for i, pattern := range f.TitlePatterns {
			likes[i] = "title LIKE ?" + pruneLikeEscape
			args = append(args, pattern)
		}
		clauses = append(clauses, "("+strings.Join(likes, " OR ")+")")
	}
	return "(" + strings.Join(clauses, " OR ") + ")", args, nil
}

// PruneProgress reports one committed batch of a prune run.
type PruneProgress struct {
	// MessagesDeleted is the running total across the run.
	MessagesDeleted int64
	// Total is PruneOptions.Total, echoed so a callback can render a
	// percentage without closing over it. Zero means "unknown".
	Total int64
	// BatchRows is how many messages this batch deleted.
	BatchRows int
	// BatchElapsed is how long this batch's transaction took.
	BatchElapsed time.Duration
	// Elapsed is the time since the run started.
	Elapsed time.Duration
}

// PruneOptions tunes a prune run.
type PruneOptions struct {
	// BatchSize is the number of messages deleted per committed
	// transaction. Zero uses DefaultPruneBatchSize; values above
	// MaxPruneBatchSize are clamped.
	BatchSize int

	// CheckpointEvery is how many committed batches pass between passive
	// WAL checkpoints. Zero uses DefaultPruneCheckpointEvery.
	CheckpointEvery int

	// Total, when positive, is the pre-counted match total handed to
	// Progress as its denominator. PruneMessagesContext does not count on
	// its own — callers already do, to show a plan before confirming.
	Total int64

	// Progress, if non-nil, is called after each batch COMMITS. It runs on
	// the calling goroutine; rate-limit inside the callback if it writes.
	Progress func(PruneProgress)

	// DeleteEmptySources removes each source in PruneFilter.SourceIDs once
	// no message references it any more.
	DeleteEmptySources bool
}

func (o PruneOptions) batchSize() int {
	switch {
	case o.BatchSize <= 0:
		return DefaultPruneBatchSize
	case o.BatchSize > MaxPruneBatchSize:
		return MaxPruneBatchSize
	default:
		return o.BatchSize
	}
}

func (o PruneOptions) checkpointEvery() int64 {
	if o.CheckpointEvery <= 0 {
		return DefaultPruneCheckpointEvery
	}
	return int64(o.CheckpointEvery)
}

// PruneResult reports what a prune run removed.
type PruneResult struct {
	MessagesDeleted      int64
	ConversationsDeleted int64
	SourcesDeleted       int64
	// Batches is the number of committed message batches.
	Batches int64
	// Interrupted is true when the run stopped early because its context
	// was cancelled. Everything counted above is already committed, and
	// re-running the same command resumes from where this one stopped.
	Interrupted bool
}

// CountPruneMatches returns how many messages the filter currently targets.
func (s *Store) CountPruneMatches(ctx context.Context, filter PruneFilter) (int64, error) {
	where, args, err := filter.messagePredicate("m")
	if err != nil {
		return 0, err
	}
	var count int64
	if err := s.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM messages m WHERE "+where, args...,
	).Scan(&count); err != nil {
		return 0, fmt.Errorf("count prune matches: %w", err)
	}
	return count, nil
}

// PruneMatchCounts is a selector's scope split by authorship, so an operator
// can see how much human-written mail sits inside a bot channel before
// deciding whether to run with BotsOnly.
type PruneMatchCounts struct {
	// Total is every message in the selector's scope, IGNORING BotsOnly.
	Total int64
	// Bots is the subset with no human author — what BotsOnly would delete.
	Bots int64
	// Humans is Total - Bots: what BotsOnly would preserve, and what a run
	// WITHOUT BotsOnly would destroy.
	Humans int64
}

// CountPruneMatchesSplit returns the filter's scope split into bot-authored
// and human-authored messages. PruneFilter.BotsOnly is deliberately ignored:
// the point of the split is to show both halves of the same scope, so the
// caller can report what each choice of that flag would do.
func (s *Store) CountPruneMatchesSplit(
	ctx context.Context, filter PruneFilter,
) (PruneMatchCounts, error) {
	var counts PruneMatchCounts

	scope := filter
	scope.BotsOnly = false
	where, args, err := scope.messagePredicate("m")
	if err != nil {
		return counts, err
	}
	// One scan produces both numbers; counting twice would scan twice and
	// could straddle a concurrent write, reporting a negative human count.
	if err := s.db.QueryRowContext(ctx,
		"SELECT COUNT(*), COALESCE(SUM(CASE WHEN "+pruneBotSenderExpr("m")+
			" THEN 1 ELSE 0 END), 0) FROM messages m WHERE "+where, args...,
	).Scan(&counts.Total, &counts.Bots); err != nil {
		return PruneMatchCounts{}, fmt.Errorf("count prune matches by author: %w", err)
	}
	counts.Humans = counts.Total - counts.Bots
	return counts, nil
}

// PruneMessages permanently removes every message the filter targets, plus
// the conversations left empty behind them. See PruneMessagesContext.
func (s *Store) PruneMessages(filter PruneFilter, opts PruneOptions) (PruneResult, error) {
	return s.PruneMessagesContext(context.Background(), filter, opts)
}

// PruneMessagesContext permanently removes every message the filter targets,
// in bounded batches that EACH COMMIT ON THEIR OWN. That is the whole point
// of this method rather than a cascade DELETE: on a large archive a single
// unbounded delete transaction grows the SQLite WAL by gigabytes and can run
// for hours without ever reaching COMMIT, so nothing is durable until the
// very end and an interrupt throws all of it away. Here every batch is
// durable the moment it lands, an interrupt costs at most one batch, and
// re-running the same filter resumes.
//
// After the message loop it deletes the conversations in the filter's scope
// that no longer hold any message, and — with PruneOptions.DeleteEmptySources
// — each selected source that no longer holds any message. Both tests are
// "holds no message at all", so a conversation or source that kept rows the
// filter did not target (PruneFilter.BotsOnly, most obviously) stays.
//
// Attachment BLOBS ON DISK ARE NOT TOUCHED. Only the database rows go; the
// content-addressed files are reclaimed by attachment maintenance.
//
// This is irreversible. The caller is responsible for backups.
func (s *Store) PruneMessagesContext(
	ctx context.Context, filter PruneFilter, opts PruneOptions,
) (PruneResult, error) {
	var result PruneResult

	where, whereArgs, err := filter.messagePredicate("m")
	if err != nil {
		return result, err
	}

	batchSize := opts.batchSize()
	checkpointEvery := opts.checkpointEvery()

	// Descending id order matters twice over. It puts children before
	// parents — a Slack thread reply is inserted after its root and so
	// carries a higher id — which keeps the reply_to_message_id repair below
	// to a minimum. And it lets the loop carry a descending cursor: every id
	// at or above the last batch's low-water mark has already been deleted,
	// so the next batch resumes the scan instead of restarting it from the
	// top of the table. Without the cursor a prune of rows interleaved
	// through a 13M-row table rescans the whole tail on every batch.
	selectSQL := "SELECT m.id FROM messages m WHERE " + where +
		" AND m.id < ? ORDER BY m.id DESC LIMIT ?"

	start := time.Now()
	cursor := int64(math.MaxInt64)
	for {
		if ctx.Err() != nil {
			// Cancellation is not a failure here: every batch already
			// committed, so the run reports how far it got and the caller
			// re-runs to continue. Returning ctx.Err() would make a clean
			// Ctrl-C look like a broken prune.
			result.Interrupted = true
			return result, nil //nolint:nilerr // interruption is reported in the result, not as an error
		}
		batchStart := time.Now()
		args := make([]any, 0, len(whereArgs)+2)
		args = append(args, whereArgs...)
		args = append(args, cursor, batchSize)

		deleted, lowestID, err := s.pruneMessageBatch(ctx, selectSQL, args)
		if err != nil {
			if isPruneCancellation(ctx, err) {
				result.Interrupted = true
				return result, nil
			}
			return result, err
		}
		if lowestID == 0 {
			break
		}
		cursor = lowestID
		result.MessagesDeleted += int64(deleted)
		result.Batches++

		if opts.Progress != nil {
			opts.Progress(PruneProgress{
				MessagesDeleted: result.MessagesDeleted,
				Total:           opts.Total,
				BatchRows:       deleted,
				BatchElapsed:    time.Since(batchStart),
				Elapsed:         time.Since(start),
			})
		}

		// Fold the WAL back into the database periodically. A prune that
		// never checkpoints leaves every batch's pages in the WAL, which is
		// how an earlier unbounded delete reached 11 GB of WAL; PASSIVE
		// never blocks a reader, so this is safe to run mid-prune.
		if result.Batches%checkpointEvery == 0 {
			// Best effort: a checkpoint that loses a race with another
			// connection is retried by the next one, and a handle broken
			// badly enough to fail here fails the next batch too, where the
			// error is not swallowed.
			_ = s.dialect.CheckpointWALPassive(ctx, s.db.DB)
		}
	}

	conversations, err := s.pruneEmptyConversations(ctx, filter, batchSize)
	result.ConversationsDeleted = conversations
	if err != nil {
		if isPruneCancellation(ctx, err) {
			result.Interrupted = true
			return result, nil
		}
		return result, err
	}

	if opts.DeleteEmptySources {
		for _, sourceID := range filter.SourceIDs {
			removed, err := s.DeleteSourceIfEmpty(ctx, sourceID)
			if err != nil {
				if isPruneCancellation(ctx, err) {
					result.Interrupted = true
					return result, nil
				}
				return result, err
			}
			if removed {
				result.SourcesDeleted++
			}
		}
	}

	_ = s.dialect.CheckpointWALPassive(context.WithoutCancel(ctx), s.db.DB)
	return result, nil
}

// pruneMessageBatch deletes one batch inside its own transaction and returns
// how many rows went and the lowest id it touched (the next cursor). A zero
// count with a zero id means the selection is exhausted.
func (s *Store) pruneMessageBatch(
	ctx context.Context, selectSQL string, args []any,
) (deleted int, lowestID int64, err error) {
	// runMaintenance owns the transaction and, on PostgreSQL, clears the
	// pool-wide statement_timeout for it. The batch is bounded, so unlike
	// the cascade deletes elsewhere in the package that clearing is a
	// safety margin rather than a necessity.
	err = s.runMaintenance(ctx, func(ctx context.Context, tx *loggedTx) error {
		deleted, lowestID = 0, 0

		ids, err := scanPruneIDs(ctx, tx, selectSQL, args)
		if err != nil {
			return fmt.Errorf("select prune batch: %w", err)
		}
		if len(ids) == 0 {
			return nil
		}
		// The select is ordered descending, so the last id is the lowest.
		lowestID = ids[len(ids)-1]
		placeholders := placeholderList(len(ids))
		idArgs := anySlice(ids)

		// Break inbound thread links first. messages.reply_to_message_id is
		// a self-reference with NO ON DELETE action and foreign keys are
		// enforced (_foreign_keys=ON), so a single surviving referrer would
		// abort the whole batch. Nulling the column is the right repair
		// either way: the parent it named is about to stop existing.
		if _, err := tx.ExecContext(ctx,
			"UPDATE messages SET reply_to_message_id = NULL WHERE reply_to_message_id IN ("+
				placeholders+")", idArgs...,
		); err != nil {
			return fmt.Errorf("clear reply references: %w", err)
		}

		// messages_fts is a STANDALONE fts5 table with no triggers, so the
		// message delete below does not reach it. Leaving the documents
		// behind would keep pruned messages returning as search hits.
		if ftsSQL := s.dialect.FTSDeleteByMessageIDsSQL(placeholders); ftsSQL != "" && s.fts5Available {
			if _, err := tx.ExecContext(ctx, ftsSQL, idArgs...); err != nil {
				return fmt.Errorf("delete FTS rows: %w", err)
			}
		}

		// Cascades to message_bodies, message_raw, message_recipients,
		// message_labels, reactions, and attachments.
		res, err := tx.ExecContext(ctx,
			"DELETE FROM messages WHERE id IN ("+placeholders+")", idArgs...)
		if err != nil {
			return fmt.Errorf("delete messages: %w", err)
		}
		affected, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("count deleted messages: %w", err)
		}
		deleted = int(affected)
		return nil
	})
	if err != nil {
		return 0, 0, err
	}
	return deleted, lowestID, nil
}

// pruneEmptyConversations deletes the conversations in the filter's scope
// that hold no message any more, in the same one-transaction-per-batch shape
// as the message loop.
//
// The emptiness test is "no message at all", not "no pruned message", which
// is what makes this safe under BotsOnly: a log channel that still holds the
// engineers' own messages keeps its conversation row, and only the channels
// the prune actually emptied are removed.
func (s *Store) pruneEmptyConversations(
	ctx context.Context, filter PruneFilter, batchSize int,
) (int64, error) {
	where, whereArgs, err := filter.conversationPredicate()
	if err != nil {
		return 0, err
	}
	selectSQL := "SELECT id FROM conversations WHERE " + where +
		" AND id < ? AND NOT EXISTS (SELECT 1 FROM messages m" +
		" WHERE m.conversation_id = conversations.id)" +
		" ORDER BY id DESC LIMIT ?"

	var total int64
	cursor := int64(math.MaxInt64)
	for {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		args := make([]any, 0, len(whereArgs)+2)
		args = append(args, whereArgs...)
		args = append(args, cursor, batchSize)

		var (
			deleted  int64
			lowestID int64
		)
		err := s.runMaintenance(ctx, func(ctx context.Context, tx *loggedTx) error {
			deleted, lowestID = 0, 0
			ids, err := scanPruneIDs(ctx, tx, selectSQL, args)
			if err != nil {
				return fmt.Errorf("select empty conversations: %w", err)
			}
			if len(ids) == 0 {
				return nil
			}
			lowestID = ids[len(ids)-1]
			res, err := tx.ExecContext(ctx,
				"DELETE FROM conversations WHERE id IN ("+placeholderList(len(ids))+")",
				anySlice(ids)...)
			if err != nil {
				return fmt.Errorf("delete empty conversations: %w", err)
			}
			affected, err := res.RowsAffected()
			if err != nil {
				return fmt.Errorf("count deleted conversations: %w", err)
			}
			deleted = affected
			return nil
		})
		if err != nil {
			return total, err
		}
		if lowestID == 0 {
			return total, nil
		}
		cursor = lowestID
		total += deleted
	}
}

// DeleteSourceIfEmpty removes a source row, but only while no message still
// references it. Reports whether the row went. A source that still holds
// messages is left alone and reported as false, not as an error — that is the
// expected outcome of pruning only part of a source's mail.
func (s *Store) DeleteSourceIfEmpty(ctx context.Context, sourceID int64) (bool, error) {
	res, err := s.db.ExecContext(ctx, `
		DELETE FROM sources
		WHERE id = ?
		  AND NOT EXISTS (SELECT 1 FROM messages m WHERE m.source_id = sources.id)`,
		sourceID)
	if err != nil {
		return false, fmt.Errorf("delete source %d: %w", sourceID, err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("count deleted source %d: %w", sourceID, err)
	}
	return affected > 0, nil
}

// scanPruneIDs runs an id-only SELECT and collects the results in order.
func scanPruneIDs(
	ctx context.Context, tx *loggedTx, query string, args []any,
) ([]int64, error) {
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// placeholderList returns n comma-separated `?` markers.
func placeholderList(n int) string {
	return strings.TrimSuffix(strings.Repeat("?,", n), ",")
}

// anySlice widens ids for variadic bind-argument use.
func anySlice(ids []int64) []any {
	args := make([]any, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	return args
}

// isPruneCancellation reports whether err arrived because the caller
// cancelled, rather than because something failed. The driver surfaces a
// cancelled statement in several shapes depending on where the cancellation
// landed, so the caller's own ctx is the reliable signal; either way the
// batch has already rolled back and the run stops, reporting what earlier
// batches committed.
func isPruneCancellation(ctx context.Context, err error) bool {
	if err == nil {
		return false
	}
	return ctx.Err() != nil ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded)
}
