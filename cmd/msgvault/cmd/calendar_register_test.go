package cmd

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/msgvault/internal/gcal"
	"go.kenn.io/msgvault/internal/testutil"
)

// The service-account path of add-calendar skips consent entirely and relies
// on registerCalendarsAndReport to enumerate calendars, create the gcal source
// rows, and report them — so that helper is exercised directly with a mock API.
func TestRegisterCalendarsAndReport_RegistersSourcesAndReports(t *testing.T) {
	st := testutil.NewTestStore(t)
	api := gcal.NewMockAPI()
	api.Calendars = []gcal.Calendar{
		{ID: "primary", Summary: "Alice", AccessRole: "owner"},
		{ID: "team@group.calendar.google.com", Summary: "Team", AccessRole: "writer"},
		{ID: "holidays", Summary: "Holidays", AccessRole: "reader"}, // excluded by default role filter
	}

	var out bytes.Buffer
	err := registerCalendarsAndReport(context.Background(), &out, st, api,
		"alice@example.com", "acme", true)
	require.NoError(t, err)

	for _, id := range []string{"alice@example.com/primary", "alice@example.com/team@group.calendar.google.com"} {
		src, err := st.GetSourceByIdentifier(id)
		require.NoError(t, err, id)
		assert.Equal(t, "gcal", src.SourceType)
	}
	_, err = st.GetSourceByIdentifier("alice@example.com/holidays")
	assert.Error(t, err, "reader-only calendar is not registered without --all-calendars")

	assert.Contains(t, out.String(), "Registered 2 calendar(s) for alice@example.com")
	assert.Contains(t, out.String(), "Next: ")
}

func TestRegisterCalendarsAndReport_NoMatchIsNotAnError(t *testing.T) {
	st := testutil.NewTestStore(t)
	api := gcal.NewMockAPI()
	api.Calendars = []gcal.Calendar{{ID: "holidays", AccessRole: "reader"}}

	var out bytes.Buffer
	require.NoError(t, registerCalendarsAndReport(context.Background(), &out, st, api,
		"alice@example.com", "", false))
	assert.Contains(t, out.String(), "No calendars matched the filter")
}
