package tldv

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListMeetings_ParamsAndPagination(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	var calls []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal("test-key", r.Header.Get("x-api-key"))
		assert.Empty(r.Header.Get("Authorization"), "tldv authenticates with x-api-key, not Bearer")
		assert.Equal("/v1alpha1/meetings", r.URL.Path)
		calls = append(calls, r.URL.RawQuery)
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Query().Get("page") {
		case "1":
			_, _ = fmt.Fprint(w, `{"page":1,"pages":2,"total":2,"pageSize":1,"results":[{"id":"m1","name":"First","happenedAt":"2026-06-01T15:02:11Z","url":"https://tldv.io/m1","organizer":{"name":"Alice Smith","email":"alice@example.com"},"invitees":[{"name":"Bob Jones","email":"bob@example.com"}]}]}`)
		default:
			_, _ = fmt.Fprint(w, `{"page":2,"pages":2,"total":2,"pageSize":1,"results":[{"id":"m2","name":"Second","happenedAt":"2026-06-02T09:15:00Z","url":"https://tldv.io/m2","organizer":{"name":"","email":"alice@example.com"},"invitees":[]}]}`)
		}
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "test-key")
	dateFrom := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)

	page1, err := c.ListMeetings(context.Background(), ListMeetingsParams{Page: 1, Limit: 50, DateFrom: dateFrom})
	require.NoError(err)
	require.Len(page1.Results, 1)
	assert.Equal("m1", page1.Results[0].ID)
	assert.Equal("Alice Smith", page1.Results[0].Organizer.Name)
	assert.Equal(2, page1.Pages)
	assert.Equal(time.Date(2026, 6, 1, 15, 2, 11, 0, time.UTC), page1.Results[0].HappenedAt)

	page2, err := c.ListMeetings(context.Background(), ListMeetingsParams{Page: 2, Limit: 50, DateFrom: dateFrom})
	require.NoError(err)
	require.Len(page2.Results, 1)
	assert.Equal("m2", page2.Results[0].ID)
	assert.Empty(page2.Results[0].Organizer.Name)

	require.Len(calls, 2)
	assert.Equal("dateFrom=2026-05-01T00%3A00%3A00Z&limit=50&page=1", calls[0])
	assert.Equal("dateFrom=2026-05-01T00%3A00%3A00Z&limit=50&page=2", calls[1])
}

func TestListMeetings_LimitCappedAt50(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	var gotLimit string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotLimit = r.URL.Query().Get("limit")
		_, _ = fmt.Fprint(w, `{"page":1,"pages":1,"total":0,"pageSize":50,"results":[]}`)
	}))
	defer srv.Close()

	_, err := NewClient(srv.URL, "test-key").ListMeetings(context.Background(), ListMeetingsParams{Page: 1, Limit: 500})
	require.NoError(err)
	assert.Equal("50", gotLimit, "limit is capped at the API ceiling of 50")
}

func TestGetMeeting_DecodesAndKeepsRaw(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	fixture := `{"id":"m1","name":"Quarterly Planning Review","happenedAt":"2026-06-01T15:00:00Z","url":"https://tldv.io/m1","organizer":{"name":"Bob Jones","email":"bob@example.com"},"invitees":[{"name":"Alice Smith","email":"alice@example.com"},{"name":"","email":"carol@example.com"}]}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal("/v1alpha1/meetings/m1", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, fixture)
	}))
	defer srv.Close()

	m, err := NewClient(srv.URL, "test-key").GetMeeting(context.Background(), "m1")
	require.NoError(err)
	assert.Equal("Quarterly Planning Review", m.Name)
	assert.Equal("bob@example.com", m.Organizer.Email)
	require.Len(m.Invitees, 2)
	assert.Empty(m.Invitees[1].Name, "null-ish attendee name decodes to empty string")
	assert.JSONEq(fixture, string(m.Raw), "raw preserves the verbatim response")
}

func TestGetTranscript_DecodesOffsets(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal("/v1alpha1/meetings/m1/transcript", r.URL.Path)
		_, _ = fmt.Fprint(w, `{"id":"t1","meetingId":"m1","data":[{"speaker":"Alice Smith","text":"Hello","startTime":0,"endTime":2.5},{"speaker":"Bob Jones","text":"Hi","startTime":71.0,"endTime":73.2}]}`)
	}))
	defer srv.Close()

	tr, err := NewClient(srv.URL, "test-key").GetTranscript(context.Background(), "m1")
	require.NoError(err)
	require.NotNil(tr)
	require.Len(tr.Data, 2)
	assert.Equal("Alice Smith", tr.Data[0].Speaker)
	assert.InEpsilon(71.0, tr.Data[1].StartTime, 1e-9)
}

func TestGetTranscript_NotFoundIsAbsent(t *testing.T) {
	require := require.New(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	tr, err := NewClient(srv.URL, "test-key").GetTranscript(context.Background(), "m1")
	require.NoError(err, "a missing transcript must not fail")
	require.Nil(tr)
}

func TestGetNotes_DecodesAndKeepsRaw(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	fixture := `{"structuredNotes":[{"segmentId":"s1","timestamp":12.5,"text":"Ship X","topicId":"t1"}],"markdownContent":"## Summary\nAlice agreed to ship X","topics":[{"id":"t1","order":0,"title":"Decisions","summary":"Ship X"}]}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal("/v1alpha1/meetings/m1/notes", r.URL.Path)
		_, _ = fmt.Fprint(w, fixture)
	}))
	defer srv.Close()

	n, err := NewClient(srv.URL, "test-key").GetNotes(context.Background(), "m1")
	require.NoError(err)
	require.NotNil(n)
	assert.Equal("## Summary\nAlice agreed to ship X", n.MarkdownContent)
	require.Len(n.Topics, 1)
	assert.Equal("Decisions", n.Topics[0].Title)
	assert.JSONEq(fixture, string(n.Raw), "raw preserves the verbatim response")
}

func TestGetNotes_ErrorIsAbsent(t *testing.T) {
	require := require.New(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	n, err := NewClient(srv.URL, "test-key").GetNotes(context.Background(), "m1")
	require.NoError(err, "missing notes are optional")
	require.Nil(n)
}

func TestClient_RetriesOn429(t *testing.T) {
	require := require.New(t)

	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 1 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"page":1,"pages":1,"total":0,"pageSize":50,"results":[]}`)
	}))
	defer srv.Close()

	out, err := NewClient(srv.URL, "test-key").ListMeetings(context.Background(), ListMeetingsParams{Page: 1})
	require.NoError(err)
	require.Empty(out.Results)
	require.Equal(int32(2), hits.Load(), "expected one retry after the 429")
}

func TestClient_RetriesOn500(t *testing.T) {
	require := require.New(t)
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_, _ = fmt.Fprint(w, `{"page":1,"pages":1,"total":0,"pageSize":50,"results":[]}`)
	}))
	defer srv.Close()

	_, err := NewClient(srv.URL, "test-key").ListMeetings(context.Background(), ListMeetingsParams{Page: 1})
	require.NoError(err)
	require.Equal(int32(2), hits.Load(), "expected one retry after the 5xx")
}

func TestClient_UnauthorizedIsActionable(t *testing.T) {
	require := require.New(t)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()

	_, err := NewClient(srv.URL, "bad").ListMeetings(context.Background(), ListMeetingsParams{Page: 1})
	require.Error(err)
	require.Contains(err.Error(), "api_key")
}
