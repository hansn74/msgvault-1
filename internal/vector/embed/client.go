// Package embed implements an OpenAI-compatible /v1/embeddings HTTP client.
// It is used by the vector search pipeline to convert email text into
// embedding vectors suitable for ANN search.
package embed

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"time"

	"go.kenn.io/kit/embedclient"
	"go.kenn.io/kit/embedconfig"
	"go.kenn.io/msgvault/internal/vector"

	"golang.org/x/sync/errgroup"
)

// ErrPermanent4xx marks a non-retryable HTTP 4xx response from the
// embeddings endpoint. Use errors.Is(err, ErrPermanent4xx) to detect
// it. Kit reports the status and failure reason without echoing provider
// response bodies. HTTP 408, 429 and 5xx remain retryable.
var ErrPermanent4xx = vector.ErrPermanent4xx

// Config controls an embeddings Client. The zero value is not usable; callers
// must set Endpoint, Model, and Dimension at a minimum.
type Config struct {
	// Endpoint is the base URL including /v1 (e.g. "http://host:8080/v1").
	// The request path "/embeddings" is appended.
	Endpoint string
	// APIKey is an optional bearer token sent as Authorization: Bearer <key>.
	APIKey string
	// Model is the model name passed in the request body.
	Model string
	// DocumentPrefix is prepended to every document chunk before it is sent.
	DocumentPrefix string
	// QueryPrefix is prepended to every query before it is sent.
	QueryPrefix string
	// Dimension is the expected vector dimension. Responses whose vectors
	// differ are rejected with an error.
	Dimension int
	// Timeout is the per-request HTTP timeout. Defaults to 30s when zero.
	Timeout time.Duration
	// MaxRetries is the maximum number of HTTP attempts for a single Embed
	// call. Defaults to 3 when zero. Kit retries HTTP 408, 429, 5xx and
	// transport failures with jittered exponential backoff.
	MaxRetries int
	// BeforeRequest reauthorizes each concrete HTTP attempt. A returned error
	// is propagated without retrying. Nil leaves the client ungated.
	BeforeRequest BeforeRequestFunc
	// RejectRedirects prevents provider responses from replaying embedding input
	// to another URL. BeforeRequest clients always reject redirects as well.
	RejectRedirects bool
	// Concurrency is how many embedding requests a single Embed call may
	// have in flight at once. Defaults to 1, which preserves the original
	// one-request-per-call behaviour. Higher values split the inputs into
	// that many contiguous parts and issue them in parallel, which is a
	// large win against a GPU-backed endpoint whose per-request latency
	// dominates: one batch at a time leaves the accelerator idle between
	// round trips. Retry, dimension checks and 4xx classification are
	// unchanged and apply per part, so error semantics (including
	// ErrPermanent4xx, which the worker's downshift drain relies on) are
	// identical to the serial path.
	Concurrency int
}

// minConcurrentPart is the smallest number of inputs worth giving its own
// request. Splitting below this trades useful batching for round trips.
const minConcurrentPart = 8

// Client calls an OpenAI-compatible /v1/embeddings endpoint.
type Client struct {
	cfg  Config
	http *http.Client
}

// NewClient constructs a Client, applying defaults for Timeout, MaxRetries
// and Concurrency.
func NewClient(cfg Config) *Client {
	if cfg.Timeout == 0 {
		cfg.Timeout = 30 * time.Second
	}
	if cfg.MaxRetries == 0 {
		cfg.MaxRetries = 3
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 1
	}
	return &Client{cfg: cfg, http: newHTTPClient(cfg.Timeout, nil, cfg.RejectRedirects || cfg.BeforeRequest != nil)}
}

// Embed returns one vector per input. With Concurrency above 1 the inputs are
// split into contiguous parts embedded in parallel and reassembled in input
// order, so callers see no difference beyond the speed.
func (c *Client) Embed(ctx context.Context, inputs []string) ([][]float32, error) {
	parts := c.splitForConcurrency(inputs)
	if len(parts) == 1 {
		return c.embedOne(ctx, inputs)
	}

	results := make([][][]float32, len(parts))
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(c.cfg.Concurrency)
	for i, part := range parts {
		group.Go(func() error {
			vecs, err := c.embedOne(groupCtx, part)
			if err != nil {
				return err
			}
			results[i] = vecs
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}
	out := make([][]float32, 0, len(inputs))
	for _, part := range results {
		out = append(out, part...)
	}
	return out, nil
}

// splitForConcurrency divides inputs into at most Concurrency contiguous
// parts, never smaller than minConcurrentPart. It returns a single part when
// concurrency is disabled or the input is too small to be worth splitting.
func (c *Client) splitForConcurrency(inputs []string) [][]string {
	parts := min(c.cfg.Concurrency, len(inputs)/minConcurrentPart)
	if parts <= 1 {
		return [][]string{inputs}
	}
	out := make([][]string, 0, parts)
	size := (len(inputs) + parts - 1) / parts
	for start := 0; start < len(inputs); start += size {
		out = append(out, inputs[start:min(start+size, len(inputs))])
	}
	return out
}

// embedOne performs one embeddings request for inputs, with the retry and
// backoff policy from Config. Formerly Embed; the split happens above.
// Embed embeds document chunks in input order. Kit owns transport validation
// and retry policy; MaxRetries is the total attempt budget. Empty input is a no-op.
func (c *Client) embedOne(ctx context.Context, inputs []string) ([][]float32, error) {
	return c.embed(ctx, embedconfig.RoleDocument, inputs)
}

func (c *Client) embed(ctx context.Context, role embedconfig.Role, inputs []string) ([][]float32, error) {
	if len(inputs) == 0 {
		return nil, nil
	}
	httpClient := *c.http
	if c.cfg.BeforeRequest != nil {
		// Consent failure ends this call, including Kit retries. Each concrete
		// attempt still checks authorization at the transport boundary.
		var cancel context.CancelCauseFunc
		ctx, cancel = context.WithCancelCause(ctx)
		defer cancel(nil)
		base := httpClient.Transport
		if base == nil {
			base = http.DefaultTransport
		}
		httpClient.Transport = beforeRequestTransport{base: base, before: func(ctx context.Context) error {
			err := c.cfg.BeforeRequest(ctx)
			if err != nil {
				cancel(err)
			}
			return err
		}}
	}
	client, err := embedclient.New(embedclient.Options{
		Model: embedconfig.Model{Name: c.cfg.Model, Dimensions: c.cfg.Dimension,
			Metric: embedconfig.MetricCosine, Normalization: embedconfig.NormalizationNone},
		Roles:      embedconfig.Roles{DocumentPrefix: c.cfg.DocumentPrefix, QueryPrefix: c.cfg.QueryPrefix},
		Deployment: embedconfig.Deployment{BaseURL: c.cfg.Endpoint, TrustPrivateNetwork: true},
		// The workers already pack batches and preserve document boundaries.
		Batch:     embedconfig.Batch{Items: math.MaxInt},
		Transport: embedconfig.Transport{Timeout: c.cfg.Timeout},
		APIKey:    c.cfg.APIKey, HTTP: &httpClient,
		Retry: embedclient.Retry{
			MaxAttempts:    c.cfg.MaxRetries,
			InitialBackoff: 200 * time.Millisecond,
			MaxBackoff:     25600 * time.Millisecond,
			MaxRetryAfter:  time.Hour,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("embed: %w", err)
	}
	vectors, err := client.EmbedTexts(ctx, role, inputs)
	if err == nil {
		return vectors, nil
	}
	if cause := beforeRequestCause(err); cause != nil {
		return nil, cause
	}
	if apiErr, ok := errors.AsType[*embedclient.APIError](err); ok {
		if apiErr.StatusCode >= 300 && apiErr.StatusCode < 400 {
			return nil, ErrEmbeddingProviderRedirect
		}
		if apiErr.StatusCode >= 400 && apiErr.StatusCode < 500 && !apiErr.Retryable() {
			return nil, fmt.Errorf("%w: %w", ErrPermanent4xx, err)
		}
		return nil, fmt.Errorf("embed: %w", err)
	}
	if errors.Is(err, embedclient.ErrInvalidVector) {
		return nil, fmt.Errorf("%w: %w", vector.ErrInvalidProviderVector, err)
	}
	if _, ok := errors.AsType[*embedclient.TransportError](err); ok {
		return nil, fmt.Errorf("embed: %w", err)
	}
	if ctx.Err() != nil {
		return nil, fmt.Errorf("embed: %w", err)
	}
	return nil, fmt.Errorf("%w: %w", vector.ErrInvalidProviderShape, err)
}

// EmbedQuery embeds one query and returns its single vector.
func (c *Client) EmbedQuery(ctx context.Context, text string) ([]float32, error) {
	vecs, err := c.embed(ctx, embedconfig.RoleQuery, []string{text})
	if err != nil {
		return nil, err
	}
	if len(vecs) != 1 {
		return nil, fmt.Errorf("%w: embed query expected exactly one vector, got %d", vector.ErrInvalidProviderShape, len(vecs))
	}
	return vecs[0], nil
}

// EmbedDocuments flattens document chunks for the OpenAI-compatible request,
// then restores the original document boundaries without changing chunk order.
func (c *Client) EmbedDocuments(ctx context.Context, documents []DocumentInput) ([][][]float32, error) {
	var inputs []string
	for _, document := range documents {
		inputs = append(inputs, document.Chunks...)
	}

	vecs, err := c.embed(ctx, embedconfig.RoleDocument, inputs)
	if err != nil {
		return nil, err
	}
	if len(vecs) != len(inputs) {
		return nil, fmt.Errorf("%w: embed documents expected %d vectors, got %d", vector.ErrInvalidProviderShape, len(inputs), len(vecs))
	}

	documentVecs := make([][][]float32, len(documents))
	offset := 0
	for i, document := range documents {
		next := offset + len(document.Chunks)
		documentVecs[i] = vecs[offset:next]
		offset = next
	}
	return documentVecs, nil
}
