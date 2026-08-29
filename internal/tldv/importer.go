package tldv

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"go.kenn.io/msgvault/internal/meetingidentity"
	"go.kenn.io/msgvault/internal/store"
)

// listPageLimit is the per-page fetch size used when paging the list endpoint.
const listPageLimit = maxPageSize

// maxListPages caps list traversal so a server reporting a runaway page count
// cannot loop forever.
const maxListPages = 100000

// Importer ingests tl;dv meeting transcripts into the msgvault store.
type Importer struct {
	store  *store.Store
	client *Client
}

// NewImporter creates an Importer backed by the given store and API client.
func NewImporter(s *store.Store, c *Client) *Importer {
	return &Importer{store: s, client: c}
}

// ImportOptions controls a sync run.
type ImportOptions struct {
	// Identifier names the source row (the configured account label/email).
	Identifier string
	// AccountEmail is the configured primary identity for organizer
	// attribution. Stored aliases for the source are included automatically.
	AccountEmail string
	// Full ignores the stored happened_after watermark and re-fetches
	// everything (bounded by After when set).
	Full bool
	// Limit caps the number of meetings processed this run (0 = unlimited).
	Limit int
	// After bounds a full sync to meetings that happened on or after this
	// time (via the dateFrom filter). Setting it implies a full run.
	After time.Time
	// Progress, when set, receives one-line status updates.
	Progress func(string)
}

// ImportSummary reports what a run did. Note-oriented field names match the
// granola importer for parity with shared CLI plumbing.
type ImportSummary struct {
	SourceID       int64
	NotesProcessed int64
	NotesAdded     int64
	NotesUpdated   int64
	Errors         int64
	Duration       time.Duration
}

// syncState is the JSON cursor persisted in sync_runs.cursor_after. tl;dv
// meetings are immutable, so the only watermark needed is the maximum
// happenedAt across ingested meetings.
type syncState struct {
	// HappenedAfter is the RFC3339Nano max happenedAt across all meetings
	// ingested by the last fully-successful, unlimited run.
	HappenedAfter string `json:"happened_after"`
}

func (s syncState) marshal() string {
	b, err := json.Marshal(s)
	if err != nil {
		return "{}"
	}
	return string(b)
}

// Import runs a full or incremental import for the configured account.
func (imp *Importer) Import(ctx context.Context, opts ImportOptions) (*ImportSummary, error) {
	start := time.Now()
	src, err := imp.store.GetOrCreateSource(SourceType, opts.Identifier)
	if err != nil {
		return nil, err
	}
	sum := &ImportSummary{SourceID: src.ID}
	if err := imp.store.AddAccountIdentityContext(
		ctx,
		src.ID,
		opts.AccountEmail,
		"account-email",
	); err != nil {
		return nil, fmt.Errorf("confirm tl;dv account identity: %w", err)
	}
	accountIdentities, err := meetingidentity.ForSource(imp.store, src.ID, opts.AccountEmail)
	if err != nil {
		return nil, err
	}

	var state syncState
	if prev, perr := imp.store.GetLastSuccessfulSync(src.ID); perr == nil && prev != nil && prev.CursorAfter.Valid {
		_ = json.Unmarshal([]byte(prev.CursorAfter.String), &state)
	}

	syncID, err := imp.store.StartSync(src.ID, SourceType)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = imp.store.FailSyncWithCheckpoint(syncID, err.Error(), &store.Checkpoint{
				MessagesProcessed: sum.NotesProcessed,
				MessagesAdded:     sum.NotesAdded,
				MessagesUpdated:   sum.NotesUpdated,
				ErrorsCount:       sum.Errors,
			})
		}
	}()

	var cursorHappenedAt time.Time
	if state.HappenedAfter != "" {
		cursorHappenedAt, _ = time.Parse(time.RFC3339Nano, state.HappenedAfter)
	}

	params := ListMeetingsParams{Limit: listPageLimit}
	switch {
	case opts.Full && !opts.After.IsZero():
		params.DateFrom = opts.After
	case !opts.Full && !cursorHappenedAt.IsZero():
		// Re-fetching the boundary is safe: PersistMessage is idempotent on
		// (source_id, source_message_id), so an overlapping meeting is upserted
		// in place rather than duplicated.
		params.DateFrom = cursorHappenedAt
	}

	// maxHappenedAt tracks the new watermark. It only advances past meetings
	// that were actually ingested, and the cursor is only persisted when the
	// run had zero errors — a failed meeting would otherwise be skipped forever.
	maxHappenedAt := cursorHappenedAt
	err = imp.forEachMeeting(ctx, src.ID, opts.Identifier, accountIdentities, params, opts, sum, func(m *Meeting) {
		if m.HappenedAt.After(maxHappenedAt) {
			maxHappenedAt = m.HappenedAt
		}
	})
	if err != nil {
		return sum, err
	}

	if err = imp.store.UpdateSyncCheckpoint(syncID, &store.Checkpoint{
		MessagesProcessed: sum.NotesProcessed,
		MessagesAdded:     sum.NotesAdded,
		MessagesUpdated:   sum.NotesUpdated,
		ErrorsCount:       sum.Errors,
	}); err != nil {
		return sum, err
	}
	if err = imp.store.RecomputeConversationStats(src.ID); err != nil {
		return sum, err
	}
	if sum.Errors > 0 {
		err = fmt.Errorf("partial tl;dv sync: %d meeting(s) failed", sum.Errors)
		return sum, err
	}

	// A limited or date-bounded full run deliberately leaves meetings
	// unprocessed, so it cannot establish a safe incremental baseline even when
	// every processed meeting succeeded. The next unbounded run must traverse
	// from the prior cursor.
	cursor := state.HappenedAfter
	boundedFull := opts.Full && !opts.After.IsZero()
	if opts.Limit == 0 && !boundedFull && !maxHappenedAt.IsZero() {
		cursor = maxHappenedAt.UTC().Format(time.RFC3339Nano)
	}
	if err = imp.store.CompleteSync(syncID, syncState{HappenedAfter: cursor}.marshal()); err != nil {
		return sum, err
	}
	sum.Duration = time.Since(start)
	return sum, nil
}

// forEachMeeting pages through the list endpoint (page-number based), fetches
// each meeting in full plus its transcript and notes, ingests it, and
// reports successfully-ingested meetings to onIngested.
func (imp *Importer) forEachMeeting(ctx context.Context, sourceID int64, identifier string, accountIdentities meetingidentity.Set, params ListMeetingsParams, opts ImportOptions, sum *ImportSummary, onIngested func(*Meeting)) error {
	progress := opts.Progress
	if progress == nil {
		progress = func(string) {}
	}
	page := 1
	for {
		if page > maxListPages {
			return fmt.Errorf("list meetings: exceeded %d pages", maxListPages)
		}
		params.Page = page
		resp, err := imp.client.ListMeetings(ctx, params)
		if err != nil {
			return fmt.Errorf("list meetings: %w", err)
		}
		if len(resp.Results) == 0 {
			return nil
		}
		for _, listed := range resp.Results {
			if err := ctx.Err(); err != nil {
				return err
			}
			if opts.Limit > 0 && sum.NotesProcessed >= int64(opts.Limit) {
				return nil
			}
			sum.NotesProcessed++
			meeting, err := imp.client.GetMeeting(ctx, listed.ID)
			if err != nil {
				sum.Errors++
				progress(fmt.Sprintf("meeting %s: fetch failed: %v", listed.ID, err))
				continue
			}
			if meeting.ID == "" || meeting.ID != listed.ID {
				sum.Errors++
				progress(fmt.Sprintf("meeting %s: fetch returned invalid ID %q", listed.ID, meeting.ID))
				continue
			}
			transcript, err := imp.client.GetTranscript(ctx, listed.ID)
			if err != nil {
				sum.Errors++
				progress(fmt.Sprintf("meeting %s: transcript fetch failed: %v", listed.ID, err))
				continue
			}
			notes, err := imp.client.GetNotes(ctx, listed.ID)
			if err != nil {
				// GetNotes never returns an error, but guard defensively.
				notes = nil
			}
			added, err := imp.ingestMeeting(sourceID, identifier, accountIdentities, meeting, transcript, notes)
			if err != nil {
				sum.Errors++
				progress(fmt.Sprintf("meeting %s: ingest failed: %v", listed.ID, err))
				continue
			}
			if added {
				sum.NotesAdded++
			} else {
				sum.NotesUpdated++
			}
			onIngested(meeting)
			progress(fmt.Sprintf("imported %q (%s)", meetingTitle(meeting), listed.ID))
		}
		// Stop once the reported page count is exhausted. Guard against a
		// server that omits Pages by also stopping on an empty page (above).
		if resp.Pages > 0 && page >= resp.Pages {
			return nil
		}
		page++
	}
}

// meetingMetadata is the structured JSON stored in messages.metadata.
type meetingMetadata struct {
	Platform       string `json:"platform"`
	MeetingID      string `json:"meeting_id"`
	URL            string `json:"url,omitempty"`
	HappenedAt     string `json:"happened_at,omitempty"`
	OrganizerEmail string `json:"organizer_email,omitempty"`
	SegmentCount   int    `json:"transcript_segments,omitempty"`
	TopicCount     int    `json:"note_topics,omitempty"`
	AccountID      string `json:"account_identifier,omitempty"`
}

// rawArchive is the verbatim message_raw blob: the meeting detail response
// plus the fetched transcript and notes responses, each preserved as received
// where possible.
type rawArchive struct {
	Meeting    json.RawMessage `json:"meeting"`
	Transcript json.RawMessage `json:"transcript,omitempty"`
	Notes      json.RawMessage `json:"notes,omitempty"`
}

// ingestMeeting persists one meeting through the canonical write path.
// Idempotent via PersistMessage's ON CONFLICT(source_id, source_message_id).
// Returns whether the message row was newly inserted.
func (imp *Importer) ingestMeeting(sourceID int64, identifier string, accountIdentities meetingidentity.Set, m *Meeting, tr *Transcript, n *Notes) (bool, error) {
	existing, err := imp.store.MessageExistsBatch(sourceID, []string{m.ID})
	if err != nil {
		return false, fmt.Errorf("lookup existing meeting: %w", err)
	}
	_, existed := existing[m.ID]

	organizerEmail := normalizeEmail(m.Organizer.Email)
	organizerName := m.Organizer.Name

	var senderID int64
	if organizerEmail != "" {
		id, err := imp.store.EnsureParticipant(organizerEmail, organizerName, emailDomain(organizerEmail))
		if err != nil {
			return false, fmt.Errorf("organizer participant: %w", err)
		}
		senderID = id
	}

	var attendeeIDs []int64
	var attendeeNames []string
	var attendeeEmails []string
	for _, a := range attendees(m) {
		pid, err := imp.store.EnsureParticipant(a.Email, a.Name, emailDomain(a.Email))
		if err != nil {
			return false, fmt.Errorf("attendee participant: %w", err)
		}
		attendeeIDs = append(attendeeIDs, pid)
		attendeeNames = append(attendeeNames, a.Name)
		attendeeEmails = append(attendeeEmails, a.Email)
	}

	title := meetingTitle(m)
	participants := make([]store.ConversationParticipantRef, 0, len(attendeeIDs))
	for _, participantID := range attendeeIDs {
		participants = append(participants, store.ConversationParticipantRef{ParticipantID: participantID, Role: "member"})
	}

	body := buildBody(m, tr, n)
	fromMe := organizerEmail != "" && accountIdentities.Contains(organizerEmail)
	sentAt := m.HappenedAt.UTC()

	message := &store.Message{
		SourceID:                sourceID,
		SourceMessageID:         m.ID,
		MessageType:             MessageType,
		SentAt:                  sql.NullTime{Time: sentAt, Valid: !sentAt.IsZero()},
		SenderID:                sql.NullInt64{Int64: senderID, Valid: senderID != 0},
		IsFromMe:                fromMe,
		IdentityDerivedIsFromMe: fromMe,
		Subject:                 sql.NullString{String: title, Valid: title != ""},
		Snippet:                 sql.NullString{String: snippet(body), Valid: body != ""},
		SizeEstimate:            int64(len(body)),
	}

	metaJSON, err := json.Marshal(buildMetadata(m, tr, n, identifier, organizerEmail))
	if err != nil {
		return false, fmt.Errorf("marshal metadata: %w", err)
	}
	metadata := sql.NullString{String: string(metaJSON), Valid: true}

	raw, err := buildRawArchive(m, tr, n)
	if err != nil {
		return false, err
	}

	// Replace recipients unconditionally (even with empty sets) so a re-sync
	// that lost its organizer or attendees clears the stale rows (granola
	// precedent).
	var fromIDs []int64
	var fromNames []string
	if senderID != 0 {
		fromIDs = []int64{senderID}
		fromNames = []string{organizerName}
	}
	// FTS: raw attendee emails go ONLY through the toAddrs column, never the
	// body, so ranking doesn't double-count them.
	fts := &store.FTSDoc{
		Subject:  title,
		Body:     body,
		FromAddr: organizerEmail,
		ToAddrs:  strings.Join(attendeeEmails, " "),
	}
	if _, err := imp.store.PersistMessage(&store.MessagePersistData{
		Message: message,
		Conversation: &store.ConversationPersistData{
			SourceConversationID: "meeting:" + m.ID,
			ConversationType:     ConversationType,
			Title:                title,
			Participants:         participants,
		},
		Metadata:  &metadata,
		BodyText:  sql.NullString{String: body, Valid: body != ""},
		RawMIME:   raw,
		RawFormat: RawFormat,
		Recipients: []store.RecipientSet{
			{Type: "from", ParticipantIDs: fromIDs, DisplayNames: fromNames},
			{Type: "to", ParticipantIDs: attendeeIDs, DisplayNames: attendeeNames},
		},
		PreserveLabels: true,
		FTS:            fts,
	}); err != nil {
		return false, fmt.Errorf("persist meeting: %w", err)
	}

	return !existed, nil
}

// buildRawArchive assembles the verbatim archive blob, preferring the raw
// response bytes and falling back to re-marshaling the decoded structs.
func buildRawArchive(m *Meeting, tr *Transcript, n *Notes) ([]byte, error) {
	arch := rawArchive{Meeting: m.Raw}
	if len(arch.Meeting) == 0 {
		encoded, err := json.Marshal(m)
		if err != nil {
			return nil, fmt.Errorf("marshal raw meeting: %w", err)
		}
		arch.Meeting = encoded
	}
	if tr != nil {
		if len(tr.Raw) > 0 {
			arch.Transcript = tr.Raw
		} else if encoded, err := json.Marshal(tr); err == nil {
			arch.Transcript = encoded
		}
	}
	if n != nil {
		if len(n.Raw) > 0 {
			arch.Notes = n.Raw
		} else if encoded, err := json.Marshal(n); err == nil {
			arch.Notes = encoded
		}
	}
	raw, err := json.Marshal(arch)
	if err != nil {
		return nil, fmt.Errorf("marshal raw archive: %w", err)
	}
	return raw, nil
}

// attendees returns the recipient list from the meeting invitees. Entries
// without an email are dropped.
func attendees(m *Meeting) []Person {
	var out []Person
	for _, a := range m.Invitees {
		if e := normalizeEmail(a.Email); e != "" {
			out = append(out, Person{Name: a.Name, Email: e})
		}
	}
	return out
}

func buildMetadata(m *Meeting, tr *Transcript, n *Notes, identifier, organizerEmail string) meetingMetadata {
	meta := meetingMetadata{
		Platform:       SourceType,
		MeetingID:      m.ID,
		URL:            m.URL,
		OrganizerEmail: organizerEmail,
		AccountID:      identifier,
	}
	if !m.HappenedAt.IsZero() {
		meta.HappenedAt = m.HappenedAt.UTC().Format(time.RFC3339)
	}
	if tr != nil {
		meta.SegmentCount = len(tr.Data)
	}
	if n != nil {
		meta.TopicCount = len(n.Topics)
	}
	return meta
}

func normalizeEmail(email string) string {
	return strings.ToLower(strings.TrimSpace(email))
}

func emailDomain(email string) string {
	if i := strings.LastIndex(email, "@"); i >= 0 {
		return email[i+1:]
	}
	return ""
}
