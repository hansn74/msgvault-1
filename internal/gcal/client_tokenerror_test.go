package gcal

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
)

// unauthorizedTokenSource mimics a service account whose domain-wide
// delegation does not cover the requested scope: Google answers the token
// exchange with 401 unauthorized_client before any API request is made.
type unauthorizedTokenSource struct{ calls int }

func (u *unauthorizedTokenSource) Token() (*oauth2.Token, error) {
	u.calls++
	return nil, &oauth2.RetrieveError{
		Response: &http.Response{StatusCode: http.StatusUnauthorized},
		Body:     []byte(`{"error":"unauthorized_client"}`),
	}
}

// A credential failure must surface immediately with Google's error text —
// not be retried as a network error for ten minutes of backoff.
func TestRequest_TokenSourceErrorFailsFast(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Error("API must not be reached when the token exchange fails")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ts := &unauthorizedTokenSource{}
	c := NewClient(ts, WithBaseURL(srv.URL))

	start := time.Now()
	_, err := c.ListCalendars(context.Background(), "")
	require.Error(t, err)

	var rerr *oauth2.RetrieveError
	assert.True(t, errors.As(err, &rerr), "the oauth2 RetrieveError must be preserved in the chain")
	assert.Contains(t, err.Error(), "unauthorized_client")
	assert.Equal(t, 1, ts.calls, "no retry on a credential error")
	assert.Less(t, time.Since(start), 2*time.Second, "must not back off")
}
