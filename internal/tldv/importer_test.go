package tldv

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/msgvault/internal/store"
	"go.kenn.io/msgvault/internal/testutil"
)

// fakeAPI serves the list, detail, transcript, and notes endpoints from
// in-memory bytes. Payloads can be swapped mid-test to simulate server edits.
type fakeAPI struct {
	mu         sync.Mutex
	orderedIDs []string
	detail     map[string][]byte
	transcript map[string][]byte
	notes      map[string][]byte
	fail       map[string]bool // meeting ID -> serve 404 on detail
}

func newFakeAPI() *fakeAPI {
	return &fakeAPI{
		detail:     map[string][]byte{},
		transcript: map[string][]byte{},
		notes:      map[string][]byte{},
		fail:       map[string]bool{},
	}
}

func (f *fakeAPI) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")

		if r.URL.Path == "/v1alpha1/meetings" {
			results := make([]json.RawMessage, 0, len(f.orderedIDs))
			for _, id := range f.orderedIDs {
				results = append(results, f.detail[id])
			}
			resp, _ := json.Marshal(map[string]any{
				"page": 1, "pages": 1, "total": len(results), "pageSize": len(results), "results": results,
			})
			_, _ = w.Write(resp)
			return
		}

		rest := strings.TrimPrefix(r.URL.Path, "/v1alpha1/meetings/")
		switch {
		case strings.HasSuffix(rest, "/transcript"):
			id := strings.TrimSuffix(rest, "/transcript")
			if body, ok := f.transcript[id]; ok {
				_, _ = w.Write(body)
				return
			}
			http.Error(w, "not found", http.StatusNotFound)
		case strings.HasSuffix(rest, "/notes"):
			id := strings.TrimSuffix(rest, "/notes")
			if body, ok := f.notes[id]; ok {
				_, _ = w.Write(body)
				return
			}
			http.Error(w, "not found", http.StatusNotFound)
		default:
			id := rest
			if f.fail[id] {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			if body, ok := f.detail[id]; ok {
				_, _ = w.Write(body)
				return
			}
			http.Error(w, "not found", http.StatusNotFound)
		}
	})
}

func newTestImporter(t *testing.T, api *fakeAPI) (*Importer, *store.Store) {
	t.Helper()
	srv := httptest.NewServer(api.handler())
	t.Cleanup(srv.Close)
	st := testutil.NewTestStore(t)
	return NewImporter(st, NewClient(srv.URL, "test-key")), st
}

// meetingFixture is a representative meeting organized by the account holder,
// with two invitees, a three-segment transcript, and AI notes.
func meetingFixture() *fakeAPI {
	api := newFakeAPI()
	api.orderedIDs = []string{"m1"}
	api.detail["m1"] = []byte(`{
		"id":"m1",
		"name":"Quarterly Planning Review",
		"happenedAt":"2026-06-01T15:00:00Z",
		"url":"https://tldv.io/m1",
		"organizer":{"name":"Me User","email":"me@example.com"},
		"invitees":[
			{"name":"Alice Smith","email":"alice@example.com"},
			{"name":"Bob Jones","email":"bob@example.com"}
		]
	}`)
	api.transcript["m1"] = []byte(`{
		"id":"t1","meetingId":"m1","data":[
			{"speaker":"Me User","text":"Let's get started with the quarterly review.","startTime":0,"endTime":4.5},
			{"speaker":"Bob Jones","text":"Sounds good. I have the budget numbers ready.","startTime":71.0,"endTime":75.2},
			{"speaker":"","text":"The deadline for phase one is July fifteenth.","startTime":3692.0,"endTime":3698.0}
		]
	}`)
	api.notes["m1"] = []byte(`{
		"structuredNotes":[
			{"segmentId":"s1","timestamp":120.0,"text":"Agreed on three priorities","topicId":"t1"}
		],
		"markdownContent":"## Summary\nAgreed on three priorities. Budget approved.",
		"topics":[
			{"id":"t1","order":0,"title":"Priorities","summary":"Three priorities agreed"},
			{"id":"t2","order":1,"title":"Budget","summary":"Budget approved"}
		]
	}`)
	return api
}

func TestImport_RoundTrip(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)
	api := meetingFixture()
	imp, st := newTestImporter(t, api)

	sum, err := imp.Import(context.Background(), ImportOptions{
		Identifier:   "me@example.com",
		AccountEmail: "me@example.com",
	})
	require.NoError(err)
	assert.EqualValues(1, sum.NotesProcessed)
	assert.EqualValues(1, sum.NotesAdded)
	assert.EqualValues(0, sum.Errors)

	// Message row: type, subject, sent_at from happenedAt, is_from_me true
	// because the organizer is the account holder.
	var subject string
	var sentAt time.Time
	var fromMe bool
	require.NoError(st.DB().QueryRow(st.Rebind(
		`SELECT subject, sent_at, is_from_me FROM messages WHERE source_message_id = ?`),
		"m1").Scan(&subject, &sentAt, &fromMe))
	assert.Equal("Quarterly Planning Review", subject)
	assert.Equal(time.Date(2026, 6, 1, 15, 0, 0, 0, time.UTC), sentAt.UTC())
	assert.True(fromMe, "organizer me@example.com is the account identifier")

	// Conversation: one per meeting, type "meeting".
	var convType, convTitle string
	require.NoError(st.DB().QueryRow(st.Rebind(`
		SELECT c.conversation_type, c.title FROM conversations c
		JOIN messages m ON m.conversation_id = c.id
		WHERE m.source_message_id = ?`), "m1").Scan(&convType, &convTitle))
	assert.Equal("meeting", convType)
	assert.Equal("Quarterly Planning Review", convTitle)

	// Body carries attendee names, highlight text, and offset-stamped
	// transcript lines; raw attendee emails stay out of the body.
	var body string
	require.NoError(st.DB().QueryRow(st.Rebind(`
		SELECT mb.body_text FROM message_bodies mb
		JOIN messages m ON m.id = mb.message_id
		WHERE m.source_message_id = ?`), "m1").Scan(&body))
	assert.Contains(body, "Attendees: Alice Smith, Bob Jones")
	assert.Contains(body, "Summary:")
	assert.Contains(body, "Agreed on three priorities. Budget approved.")
	assert.Contains(body, "[00:00] Me User: Let's get started with the quarterly review.")
	assert.Contains(body, "[01:11] Bob Jones: Sounds good. I have the budget numbers ready.")
	assert.Contains(body, "[1:01:32] Speaker: The deadline for phase one is July fifteenth.")
	assert.NotContains(body, "alice@example.com", "raw attendee emails stay out of the body")

	// Recipients: from = organizer me, to = both invitees.
	var fromEmail string
	require.NoError(st.DB().QueryRow(st.Rebind(`
		SELECT p.email_address FROM message_recipients mr
		JOIN participants p ON p.id = mr.participant_id
		JOIN messages m ON m.id = mr.message_id
		WHERE m.source_message_id = ? AND mr.recipient_type = 'from'`),
		"m1").Scan(&fromEmail))
	assert.Equal("me@example.com", fromEmail)

	var toCount int
	require.NoError(st.DB().QueryRow(st.Rebind(`
		SELECT COUNT(*) FROM message_recipients mr
		JOIN messages m ON m.id = mr.message_id
		WHERE m.source_message_id = ? AND mr.recipient_type = 'to'`),
		"m1").Scan(&toCount))
	assert.Equal(2, toCount)

	// Metadata JSON.
	var msgID int64
	require.NoError(st.DB().QueryRow(st.Rebind(
		`SELECT id FROM messages WHERE source_message_id = ?`), "m1").Scan(&msgID))
	metaNS, err := st.GetMessageMetadata(msgID)
	require.NoError(err)
	require.True(metaNS.Valid)
	var meta meetingMetadata
	require.NoError(json.Unmarshal([]byte(metaNS.String), &meta))
	assert.Equal("tldv", meta.Platform)
	assert.Equal("m1", meta.MeetingID)
	assert.Equal("me@example.com", meta.OrganizerEmail)
	assert.Equal(3, meta.SegmentCount)
	assert.Equal(2, meta.TopicCount)
	assert.Equal("me@example.com", meta.AccountID)

	// Raw archive: verbatim detail plus fetched transcript and notes, tagged
	// tldv_json.
	raw, err := st.GetMessageRaw(msgID)
	require.NoError(err)
	var archive rawArchive
	require.NoError(json.Unmarshal(raw, &archive))
	assert.JSONEq(string(api.detail["m1"]), string(archive.Meeting))
	assert.JSONEq(string(api.transcript["m1"]), string(archive.Transcript))
	assert.JSONEq(string(api.notes["m1"]), string(archive.Notes))
	var rawFormat string
	require.NoError(st.DB().QueryRow(st.Rebind(
		`SELECT raw_format FROM message_raw WHERE message_id = ?`), msgID).Scan(&rawFormat))
	assert.Equal(RawFormat, rawFormat)
}

func TestImport_IdempotentReimportUpdatesInPlace(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)
	api := meetingFixture()
	imp, st := newTestImporter(t, api)

	_, err := imp.Import(context.Background(), ImportOptions{Identifier: "me@example.com", AccountEmail: "me@example.com"})
	require.NoError(err)

	// Second run (incremental): re-fetches the boundary meeting but upserts in
	// place — no new rows, updated not added.
	sum2, err := imp.Import(context.Background(), ImportOptions{Identifier: "me@example.com", AccountEmail: "me@example.com"})
	require.NoError(err)
	assert.EqualValues(0, sum2.NotesAdded, "re-import must not add rows")
	assert.EqualValues(1, sum2.NotesUpdated, "the boundary meeting is re-fetched and upserted in place")

	var count int
	require.NoError(st.DB().QueryRow(`SELECT COUNT(*) FROM messages`).Scan(&count))
	assert.Equal(1, count, "idempotent re-import must not duplicate")

	// A server-side rename refreshes the same row.
	api.mu.Lock()
	api.detail["m1"] = []byte(strings.ReplaceAll(string(api.detail["m1"]),
		"Quarterly Planning Review", "Quarterly Planning Review v2"))
	api.mu.Unlock()

	sum3, err := imp.Import(context.Background(), ImportOptions{Identifier: "me@example.com", AccountEmail: "me@example.com"})
	require.NoError(err)
	assert.EqualValues(0, sum3.NotesAdded)
	assert.EqualValues(1, sum3.NotesUpdated)

	var subject string
	require.NoError(st.DB().QueryRow(st.Rebind(
		`SELECT subject FROM messages WHERE source_message_id = ?`), "m1").Scan(&subject))
	assert.Equal("Quarterly Planning Review v2", subject)
	require.NoError(st.DB().QueryRow(`SELECT COUNT(*) FROM messages`).Scan(&count))
	assert.Equal(1, count)
}

func TestImport_AccountIdentityControlsFromMe(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	meetingFor := func(id, organizerEmail string) []byte {
		return []byte(`{
			"id":"` + id + `",
			"name":"Meeting ` + id + `",
			"happenedAt":"2026-06-01T15:00:00Z",
			"organizer":{"name":"Org","email":"` + organizerEmail + `"},
			"invitees":[]
		}`)
	}
	api := newFakeAPI()
	api.orderedIDs = []string{"note-primary", "note-alias", "note-other"}
	api.detail["note-primary"] = meetingFor("note-primary", "USER-A@EXAMPLE.COM")
	api.detail["note-alias"] = meetingFor("note-alias", "user-b@example.com")
	api.detail["note-other"] = meetingFor("note-other", "user-c@example.com")
	imp, st := newTestImporter(t, api)

	source, err := st.GetOrCreateSource(SourceType, "work")
	require.NoError(err)
	require.NoError(st.AddAccountIdentity(source.ID, " User-B@Example.COM ", "manual"))

	sum, err := imp.Import(context.Background(), ImportOptions{
		Identifier:   "work",
		AccountEmail: " user-a@example.com ",
	})
	require.NoError(err)
	assert.Equal(source.ID, sum.SourceID)

	for _, tc := range []struct {
		id   string
		want bool
	}{
		{id: "note-primary", want: true},
		{id: "note-alias", want: true},
		{id: "note-other", want: false},
	} {
		var got bool
		require.NoError(st.DB().QueryRow(st.Rebind(
			`SELECT is_from_me FROM messages WHERE source_id = ? AND source_message_id = ?`),
			source.ID, tc.id).Scan(&got))
		assert.Equal(tc.want, got, tc.id)
	}
}

func TestImport_NormalizesSentAtToUTC(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	api := newFakeAPI()
	api.orderedIDs = []string{"m1"}
	api.detail["m1"] = []byte(`{
		"id":"m1","name":"TZ meeting","happenedAt":"2026-06-01T15:00:00-05:00",
		"organizer":{"name":"Org","email":"org@example.com"},"invitees":[]
	}`)
	imp, st := newTestImporter(t, api)

	_, err := imp.Import(context.Background(), ImportOptions{Identifier: "work", AccountEmail: "work@example.com"})
	require.NoError(err)
	var sentAt time.Time
	require.NoError(st.DB().QueryRow(`SELECT sent_at FROM messages`).Scan(&sentAt))
	assert.Equal(time.Date(2026, 6, 1, 20, 0, 0, 0, time.UTC), sentAt.UTC())
}

func TestImport_CursorAdvancesOnlyOnCleanRuns(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)
	api := newFakeAPI()
	api.orderedIDs = []string{"m1", "m2"}
	api.detail["m1"] = []byte(`{"id":"m1","name":"One","happenedAt":"2026-06-01T15:00:00Z","organizer":{"email":"org@example.com"},"invitees":[]}`)
	api.detail["m2"] = []byte(`{"id":"m2","name":"Two","happenedAt":"2026-06-02T09:50:00Z","organizer":{"email":"org@example.com"},"invitees":[]}`)
	imp, st := newTestImporter(t, api)

	cursorOf := func(sourceID int64) string {
		var blob string
		require.NoError(st.DB().QueryRow(st.Rebind(`
			SELECT cursor_after FROM sync_runs
			WHERE source_id = ? AND status = 'completed'
			ORDER BY id DESC LIMIT 1`), sourceID).Scan(&blob))
		var state syncState
		require.NoError(json.Unmarshal([]byte(blob), &state))
		return state.HappenedAfter
	}

	// Clean run: watermark = max happenedAt across both meetings.
	sum, err := imp.Import(context.Background(), ImportOptions{Identifier: "work", AccountEmail: "work@example.com"})
	require.NoError(err)
	require.EqualValues(0, sum.Errors)
	assert.Equal("2026-06-02T09:50:00Z", cursorOf(sum.SourceID))

	// Failing run: m2 detail 404s; the cursor must not advance.
	api.mu.Lock()
	api.detail["m3"] = []byte(`{"id":"m3","name":"Three","happenedAt":"2026-06-05T12:00:00Z","organizer":{"email":"org@example.com"},"invitees":[]}`)
	api.orderedIDs = []string{"m1", "m2", "m3"}
	api.fail["m2"] = true
	api.mu.Unlock()

	sum2, err := imp.Import(context.Background(), ImportOptions{Identifier: "work", AccountEmail: "work@example.com"})
	require.Error(err)
	assert.Contains(err.Error(), "partial tl;dv sync")
	assert.EqualValues(1, sum2.Errors)
	assert.Equal("2026-06-02T09:50:00Z", cursorOf(sum2.SourceID), "cursor must hold until a clean run covers the failed meeting")

	// Clean retry: cursor advances past the newest meeting.
	api.mu.Lock()
	api.fail["m2"] = false
	api.mu.Unlock()
	sum3, err := imp.Import(context.Background(), ImportOptions{Identifier: "work", AccountEmail: "work@example.com"})
	require.NoError(err)
	require.EqualValues(0, sum3.Errors)
	assert.Equal("2026-06-05T12:00:00Z", cursorOf(sum3.SourceID))
}

func TestImport_LimitDoesNotAdvanceCursor(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)
	api := newFakeAPI()
	api.orderedIDs = []string{"m1", "m2"}
	api.detail["m1"] = []byte(`{"id":"m1","name":"One","happenedAt":"2026-06-01T15:00:00Z","organizer":{"email":"org@example.com"},"invitees":[]}`)
	api.detail["m2"] = []byte(`{"id":"m2","name":"Two","happenedAt":"2026-06-02T09:50:00Z","organizer":{"email":"org@example.com"},"invitees":[]}`)
	imp, st := newTestImporter(t, api)

	sum, err := imp.Import(context.Background(), ImportOptions{Identifier: "work", AccountEmail: "work@example.com", Limit: 1})
	require.NoError(err)
	require.EqualValues(1, sum.NotesProcessed)

	var cursorJSON string
	require.NoError(st.DB().QueryRow(st.Rebind(`
		SELECT cursor_after FROM sync_runs
		WHERE source_id = ? AND status = 'completed'
		ORDER BY id DESC LIMIT 1`), sum.SourceID).Scan(&cursorJSON))
	var cursor syncState
	require.NoError(json.Unmarshal([]byte(cursorJSON), &cursor))
	assert.Empty(cursor.HappenedAfter, "limited run must preserve the prior cursor")
}

func TestImport_MissingTranscriptStillArchives(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	api := newFakeAPI()
	api.orderedIDs = []string{"m1"}
	api.detail["m1"] = []byte(`{"id":"m1","name":"No transcript yet","happenedAt":"2026-06-01T15:00:00Z","organizer":{"email":"org@example.com"},"invitees":[]}`)
	// No transcript or notes registered -> both 404.
	imp, st := newTestImporter(t, api)

	sum, err := imp.Import(context.Background(), ImportOptions{Identifier: "work", AccountEmail: "work@example.com"})
	require.NoError(err)
	assert.EqualValues(1, sum.NotesAdded)
	assert.EqualValues(0, sum.Errors)

	var count int
	require.NoError(st.DB().QueryRow(`SELECT COUNT(*) FROM messages`).Scan(&count))
	assert.Equal(1, count)
}

func TestFormatTranscriptLine(t *testing.T) {
	assert := assert.New(t)
	tests := []struct {
		seconds float64
		want    string
	}{
		{0, "[00:00] A: x"},
		{71, "[01:11] A: x"},
		{3692, "[1:01:32] A: x"},
	}
	for _, tc := range tests {
		assert.Equal(tc.want, formatTranscriptLine(offsetDuration(tc.seconds), "A", "x"))
	}
}
