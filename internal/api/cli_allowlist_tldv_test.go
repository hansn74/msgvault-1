package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// The daemon-only CLI model means a command missing from the run allowlist
// simply does not work end-to-end, with no compile-time signal — so the tl;dv
// commands' presence is asserted explicitly, alongside the granola/slack ones.
func TestCLIRunCommandAllowedTldvCommands(t *testing.T) {
	for _, args := range [][]string{
		{"add-tldv"},
		{"add-tldv", "you@example.com"},
		{"sync-tldv"},
		{"sync-tldv", "you@example.com", "--full"},
	} {
		t.Run(args[0], func(t *testing.T) {
			assert.True(t, cliRunCommandAllowed(args), "%v must be runnable via the daemon CLI", args)
		})
	}
	assert.False(t, cliRunCommandAllowed([]string{"tldv-not-a-command"}))
}
