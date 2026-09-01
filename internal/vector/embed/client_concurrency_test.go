package embed

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// echoServer returns one vector per input whose first component encodes the
// input's text, so the caller can prove parts were reassembled in order.
// It also records how many requests were in flight simultaneously.
func echoServer(t *testing.T, dim int) (*httptest.Server, *int32, *int32) {
	t.Helper()
	var inFlight, maxInFlight, requests int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requests, 1)
		cur := atomic.AddInt32(&inFlight, 1)
		for {
			prev := atomic.LoadInt32(&maxInFlight)
			if cur <= prev || atomic.CompareAndSwapInt32(&maxInFlight, prev, cur) {
				break
			}
		}
		var req struct {
			Input []string `json:"input"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		// Hold the request open briefly so parallel calls genuinely overlap.
		var wg sync.WaitGroup
		wg.Add(1)
		go func() { defer wg.Done() }()
		wg.Wait()

		type item struct {
			Embedding []float32 `json:"embedding"`
			Index     int       `json:"index"`
		}
		out := struct {
			Data []item `json:"data"`
		}{}
		for i, in := range req.Input {
			vec := make([]float32, dim)
			var n float32
			_, _ = fmt.Sscanf(in, "input-%f", &n)
			vec[0] = n
			out.Data = append(out.Data, item{Embedding: vec, Index: i})
		}
		atomic.AddInt32(&inFlight, -1)
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(out))
	}))
	t.Cleanup(srv.Close)
	return srv, &maxInFlight, &requests
}

func inputs(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("input-%d", i)
	}
	return out
}

// With Concurrency > 1 the inputs are split across several requests, and the
// vectors must still come back in input order — the worker maps them back to
// message ids positionally, so a reordering would corrupt the index.
func TestEmbed_ConcurrentPreservesOrder(t *testing.T) {
	srv, maxInFlight, requests := echoServer(t, 4)
	c := NewClient(Config{Endpoint: srv.URL + "/v1", Model: "m", Dimension: 4, Concurrency: 4})

	in := inputs(64)
	vecs, err := c.Embed(context.Background(), in)
	require.NoError(t, err)
	require.Len(t, vecs, 64)
	for i, v := range vecs {
		assert.Equal(t, float32(i), v[0], "vector %d is out of order", i)
	}
	assert.Equal(t, int32(4), atomic.LoadInt32(requests), "64 inputs split across 4 requests")
	assert.Greater(t, atomic.LoadInt32(maxInFlight), int32(1), "requests must actually overlap")
}

// Concurrency must not change behaviour for small batches: splitting 10
// inputs into 4 requests would trade batching for round trips.
func TestEmbed_SmallInputStaysSingleRequest(t *testing.T) {
	srv, _, requests := echoServer(t, 4)
	c := NewClient(Config{Endpoint: srv.URL + "/v1", Model: "m", Dimension: 4, Concurrency: 4})

	_, err := c.Embed(context.Background(), inputs(10))
	require.NoError(t, err)
	assert.Equal(t, int32(1), atomic.LoadInt32(requests))
}

// The default is unchanged: one request per Embed call.
func TestEmbed_DefaultIsSerial(t *testing.T) {
	srv, _, requests := echoServer(t, 4)
	c := NewClient(Config{Endpoint: srv.URL + "/v1", Model: "m", Dimension: 4})

	_, err := c.Embed(context.Background(), inputs(64))
	require.NoError(t, err)
	assert.Equal(t, int32(1), atomic.LoadInt32(requests))
}

// A 4xx from any part must still surface as ErrPermanent4xx — the worker's
// downshift drain keys off it, and losing that classification would turn a
// permanent failure into an endless retry.
func TestEmbed_ConcurrentPropagatesPermanent4xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "bad input", http.StatusBadRequest)
	}))
	defer srv.Close()
	c := NewClient(Config{Endpoint: srv.URL + "/v1", Model: "m", Dimension: 4, Concurrency: 4})

	_, err := c.Embed(context.Background(), inputs(64))
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrPermanent4xx)
	assert.True(t, strings.Contains(err.Error(), "400"), "status code preserved: %v", err)
}

func TestSplitForConcurrency(t *testing.T) {
	c := NewClient(Config{Endpoint: "http://x/v1", Model: "m", Dimension: 4, Concurrency: 4})
	assert.Len(t, c.splitForConcurrency(inputs(64)), 4)
	assert.Len(t, c.splitForConcurrency(inputs(32)), 4)
	assert.Len(t, c.splitForConcurrency(inputs(16)), 2, "16 inputs allow only two 8-input parts")
	assert.Len(t, c.splitForConcurrency(inputs(7)), 1)

	// Every input appears exactly once, in order.
	var flat []string
	for _, part := range c.splitForConcurrency(inputs(50)) {
		flat = append(flat, part...)
	}
	assert.Equal(t, inputs(50), flat)
}
