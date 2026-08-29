package tldv

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"go.kenn.io/msgvault/internal/httpretry"
	"golang.org/x/time/rate"
)

const (
	maxRetries    = 8
	maxRetryAfter = httpretry.ProviderMaxRetryAfter
)

// maxPageSize is the API's limit ceiling.
const maxPageSize = 50

// errNotFound is returned by get on a 404 so optional-resource callers
// (transcript, highlights) can treat a missing resource as absent instead of
// a hard error.
var errNotFound = errors.New("tldv: resource not found")

// Client is a minimal tl;dv public-API client with token-bucket rate limiting
// and Retry-After back-off. tl;dv authenticates with an x-api-key header (not
// a Bearer token).
type Client struct {
	baseURL string
	apiKey  string
	http    *http.Client
	limiter *rate.Limiter
}

// NewClient creates a Client. baseURL is injected so tests can point at
// httptest servers; pass DefaultBaseURL for production. apiKey is the key from
// tl;dv's API settings.
func NewClient(baseURL, apiKey string) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		apiKey:  apiKey,
		http:    &http.Client{Timeout: 60 * time.Second},
		limiter: rate.NewLimiter(rate.Limit(5), 25),
	}
}

// get fetches path (with query already encoded), respecting the rate limiter
// and retrying on 429/5xx with Retry-After or exponential back-off. A 404
// returns errNotFound so optional resources can be distinguished.
func (c *Client) get(ctx context.Context, path string) ([]byte, error) {
	reqURL := c.baseURL + path
	for attempt := range maxRetries {
		if err := c.limiter.Wait(ctx); err != nil {
			return nil, fmt.Errorf("wait for tldv rate limit: %w", err)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("x-api-key", c.apiKey)
		req.Header.Set("Accept", "application/json")
		resp, err := c.http.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		closeErr := resp.Body.Close()
		if readErr != nil {
			return nil, fmt.Errorf("tldv GET %s: read body: %w", reqURL, readErr)
		}
		if closeErr != nil {
			return nil, fmt.Errorf("tldv GET %s: close body: %w", reqURL, closeErr)
		}

		switch {
		case resp.StatusCode == http.StatusOK:
			return body, nil
		case resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden:
			return nil, errors.New("tldv API rejected the key: check [[tldv]] api_key in config.toml (created in tl;dv's API settings; sent as the x-api-key header)")
		case resp.StatusCode == http.StatusNotFound:
			return nil, fmt.Errorf("tldv GET %s: %w", reqURL, errNotFound)
		case resp.StatusCode == http.StatusNoContent:
			// The live API answers 204 (not 404) for a meeting with no
			// transcript/notes; report it the same way as absent.
			return nil, fmt.Errorf("tldv GET %s: %w", reqURL, errNotFound)
		case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
			wait := httpretry.RetryAfter(resp.Header.Get("Retry-After"), attempt, maxRetryAfter)
			timer := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
			}
			continue
		default:
			return nil, fmt.Errorf("tldv GET %s: status %d: %s", reqURL, resp.StatusCode, string(body))
		}
	}
	return nil, fmt.Errorf("tldv GET %s: exhausted %d retries", reqURL, maxRetries)
}

// ListMeetingsParams filters GET /v1alpha1/meetings. Zero-value fields are
// omitted. Pagination is page-number based (Page starts at 1).
type ListMeetingsParams struct {
	Page             int
	Limit            int
	DateFrom         time.Time
	DateTo           time.Time
	Query            string
	MeetingType      string // "internal" or "external"
	OnlyParticipated bool
}

// ListMeetings fetches one page of meetings.
func (c *Client) ListMeetings(ctx context.Context, p ListMeetingsParams) (*ListMeetingsOutput, error) {
	q := url.Values{}
	if p.Page > 0 {
		q.Set("page", strconv.Itoa(p.Page))
	}
	if p.Limit > 0 {
		q.Set("limit", strconv.Itoa(min(p.Limit, maxPageSize)))
	}
	if !p.DateFrom.IsZero() {
		q.Set("dateFrom", p.DateFrom.UTC().Format(time.RFC3339))
	}
	if !p.DateTo.IsZero() {
		q.Set("dateTo", p.DateTo.UTC().Format(time.RFC3339))
	}
	if p.Query != "" {
		q.Set("query", p.Query)
	}
	if p.MeetingType != "" {
		q.Set("meetingType", p.MeetingType)
	}
	if p.OnlyParticipated {
		q.Set("onlyParticipated", "true")
	}
	path := "/v1alpha1/meetings"
	if len(q) > 0 {
		path += "?" + q.Encode()
	}
	body, err := c.get(ctx, path)
	if err != nil {
		return nil, err
	}
	var out ListMeetingsOutput
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("tldv list meetings: decode: %w", err)
	}
	return &out, nil
}

// GetMeeting fetches a meeting's detail. The verbatim response body is
// preserved in Meeting.Raw.
func (c *Client) GetMeeting(ctx context.Context, meetingID string) (*Meeting, error) {
	body, err := c.get(ctx, "/v1alpha1/meetings/"+url.PathEscape(meetingID))
	if err != nil {
		return nil, err
	}
	var m Meeting
	if err := json.Unmarshal(body, &m); err != nil {
		return nil, fmt.Errorf("tldv get meeting %s: decode: %w", meetingID, err)
	}
	m.Raw = json.RawMessage(body)
	return &m, nil
}

// GetTranscript fetches a meeting's transcript. A missing transcript (404) is
// reported as a nil result with no error so a still-processing meeting is
// archived rather than failed. Raw preserves the verbatim response body.
func (c *Client) GetTranscript(ctx context.Context, meetingID string) (*Transcript, error) {
	body, err := c.get(ctx, "/v1alpha1/meetings/"+url.PathEscape(meetingID)+"/transcript")
	if err != nil {
		if errors.Is(err, errNotFound) {
			return nil, nil
		}
		return nil, err
	}
	var tr Transcript
	if err := json.Unmarshal(body, &tr); err != nil {
		return nil, fmt.Errorf("tldv get transcript %s: decode: %w", meetingID, err)
	}
	tr.Raw = json.RawMessage(body)
	return &tr, nil
}

// GetNotes fetches a meeting's AI notes (the summary endpoint that supersedes
// the deprecated /highlights). The endpoint is optional: any 404, decode
// failure, or transport error is treated as absent (nil, nil) so missing notes
// never fail a meeting.
func (c *Client) GetNotes(ctx context.Context, meetingID string) (*Notes, error) {
	body, err := c.get(ctx, "/v1alpha1/meetings/"+url.PathEscape(meetingID)+"/notes")
	if err != nil {
		return nil, nil
	}
	var n Notes
	if err := json.Unmarshal(body, &n); err != nil {
		return nil, nil
	}
	n.Raw = json.RawMessage(body)
	return &n, nil
}
