package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The tl;dv key may be supplied through an environment variable so it need not
// sit in config.toml, where an ordinary diff or dump of the settings file would
// expose it.
func TestTldvSourceResolvedAPIKey(t *testing.T) {
	t.Run("literal api_key wins", func(t *testing.T) {
		t.Setenv("MSGVAULT_TEST_TLDV_KEY", "from-environment")
		src := TldvSource{Identifier: "work", APIKey: "from-config", APIKeyEnv: "MSGVAULT_TEST_TLDV_KEY"}

		key, err := src.ResolvedAPIKey()
		require.NoError(t, err)
		assert.Equal(t, "from-config", key)
	})

	t.Run("falls back to the named environment variable", func(t *testing.T) {
		t.Setenv("MSGVAULT_TEST_TLDV_KEY", "from-environment")
		src := TldvSource{Identifier: "work", APIKeyEnv: "MSGVAULT_TEST_TLDV_KEY"}

		key, err := src.ResolvedAPIKey()
		require.NoError(t, err)
		assert.Equal(t, "from-environment", key)
	})

	t.Run("reports an unset variable rather than sending an empty key", func(t *testing.T) {
		src := TldvSource{Identifier: "work", APIKeyEnv: "MSGVAULT_TEST_TLDV_KEY_ABSENT"}

		_, err := src.ResolvedAPIKey()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "MSGVAULT_TEST_TLDV_KEY_ABSENT")
	})

	t.Run("an empty variable is treated as unset", func(t *testing.T) {
		t.Setenv("MSGVAULT_TEST_TLDV_KEY", "   ")
		src := TldvSource{Identifier: "work", APIKeyEnv: "MSGVAULT_TEST_TLDV_KEY"}

		_, err := src.ResolvedAPIKey()
		require.Error(t, err)
	})

	t.Run("neither configured", func(t *testing.T) {
		src := TldvSource{Identifier: "work"}

		_, err := src.ResolvedAPIKey()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "api_key_env")
	})

	t.Run("the error never contains the key itself", func(t *testing.T) {
		t.Setenv("MSGVAULT_TEST_TLDV_KEY", "")
		src := TldvSource{Identifier: "work", APIKeyEnv: "MSGVAULT_TEST_TLDV_KEY"}

		_, err := src.ResolvedAPIKey()
		require.Error(t, err)
		assert.NotContains(t, err.Error(), "from-config")
	})
}
