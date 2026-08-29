package tldv

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The live API returns happenedAt as JavaScript Date.toString() output rather
// than the ISO8601 the docs imply; the decoder must accept both plus date-only.
func TestAPITimestampFormats(t *testing.T) {
	cases := []struct {
		name string
		wire string
		want time.Time
	}{
		{"rfc3339", `"2026-08-28T12:30:00Z"`, time.Date(2026, 8, 28, 12, 30, 0, 0, time.UTC)},
		{"date-only", `"2026-08-28"`, time.Date(2026, 8, 28, 0, 0, 0, 0, time.UTC)},
		{
			"js-date-toString",
			`"Fri Aug 28 2026 12:30:00 GMT+0000 (Coordinated Universal Time)"`,
			time.Date(2026, 8, 28, 12, 30, 0, 0, time.UTC),
		},
		{
			"js-date-offset-zone",
			`"Fri Aug 28 2026 14:30:00 GMT+0200 (Central European Summer Time)"`,
			time.Date(2026, 8, 28, 12, 30, 0, 0, time.UTC),
		},
		{"null", `null`, time.Time{}},
		{"empty", `""`, time.Time{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var ts apiTimestamp
			require.NoError(t, json.Unmarshal([]byte(tc.wire), &ts))
			assert.True(t, tc.want.Equal(time.Time(ts)), "want %v got %v", tc.want, time.Time(ts))
		})
	}

	var ts apiTimestamp
	assert.Error(t, json.Unmarshal([]byte(`"not a date"`), &ts))
}
