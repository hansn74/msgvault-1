package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// The daemon-only CLI model means a command missing from the run allowlist
// simply does not work end-to-end, with no compile-time signal — so
// prune-messages' presence is asserted explicitly, in the shapes the command
// is actually invoked in.
func TestCLIRunCommandAllowedPruneMessages(t *testing.T) {
	for _, args := range [][]string{
		{"prune-messages"},
		{"prune-messages", "--conversation-title", "#logs-*", "--dry-run"},
		{"prune-messages", "--source", "gmail:you@example.com", "--confirmed", "--batch-size", "2000"},
	} {
		t.Run(args[0], func(t *testing.T) {
			assert.True(t, cliRunCommandAllowed(args), "%v must be runnable via the daemon CLI", args)
		})
	}
	assert.False(t, cliRunCommandAllowed([]string{"prune-messages-not-a-command"}))
}
