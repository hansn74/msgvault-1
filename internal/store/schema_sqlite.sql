-- SQLite-specific extensions (FTS5 for full-text search)

-- Full-text search index for messages.
-- Contentless FTS (content='') stores only the inverted index; the
-- application is responsible for supplying column values on INSERT and
-- DELETE. contentless_delete=1 (SQLite 3.43+) lets DELETE FROM correctly
-- remove terms from the index without needing the old values.
-- This avoids duplicating message body text (~100KB per message of
-- HTML-heavy email) that already lives in message_bodies.
CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5(
    subject,
    body,
    from_addr,
    to_addr,
    cc_addr,
    tokenize='unicode61 remove_diacritics 1',
    content='',
    contentless_delete=1
);
