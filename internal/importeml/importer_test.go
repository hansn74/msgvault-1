package importeml

import (
	"compress/gzip"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/msgvault/internal/store"
)

// openTestStore creates a temporary on-disk database for testing.
func openTestStore(t *testing.T) *store.Store {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	st, err := store.Open(dbPath)
	require.NoError(t, err, "open store")
	require.NoError(t, st.InitSchema(), "init schema")
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func countMessages(t *testing.T, st *store.Store) int {
	t.Helper()
	var n int
	require.NoError(t, st.DB().QueryRow(`SELECT COUNT(*) FROM messages`).Scan(&n), "count messages")
	return n
}

func TestDiscoverFiles(t *testing.T) {
	tests := []struct {
		name      string
		setup     func(t *testing.T) string
		recursive bool
		wantCount int
	}{
		{
			name:      "single file",
			setup:     func(t *testing.T) string { return "testdata/simple.eml" },
			wantCount: 1,
		},
		{
			name:      "directory non-recursive",
			setup:     func(t *testing.T) string { return "testdata" },
			wantCount: 3, // simple.eml, html_body.eml, no_message_id.eml
		},
		{
			name: "directory recursive",
			setup: func(t *testing.T) string {
				dir := t.TempDir()
				sub := filepath.Join(dir, "sub")
				require.NoError(t, os.MkdirAll(sub, 0755))
				for _, name := range []string{"a.eml", "b.txt"} {
					require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte("test"), 0644))
				}
				require.NoError(t, os.WriteFile(filepath.Join(sub, "c.eml"), []byte("test"), 0644))
				return dir
			},
			recursive: true,
			wantCount: 2, // a.eml + sub/c.eml (b.txt excluded)
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := tt.setup(t)
			files, err := discoverFiles([]string{path}, tt.recursive)
			require.NoError(t, err, "discoverFiles")
			assert.Len(t, files, tt.wantCount, "files: %v", files)
		})
	}
}

func TestIsEMLFile(t *testing.T) {
	tests := []struct {
		path string
		want bool
	}{
		{"message.eml", true},
		{"MESSAGE.EML", true},
		{"archive.eml.gz", true},
		{"archive.EML.GZ", true},
		{"readme.txt", false},
		{"email.mbox", false},
		{".eml", true},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			assert.Equal(t, tt.want, isEMLFile(tt.path))
		})
	}
}

func TestReadEMLFile(t *testing.T) {
	t.Run("plain eml", func(t *testing.T) {
		data, err := readEMLFile("testdata/simple.eml")
		require.NoError(t, err, "readEMLFile")
		assert.NotEmpty(t, data)
	})

	t.Run("gzipped eml", func(t *testing.T) {
		gzPath := filepath.Join(t.TempDir(), "test.eml.gz")
		content := []byte("From: test@example.com\r\nSubject: gzip test\r\n\r\nBody text\r\n")

		f, err := os.Create(gzPath)
		require.NoError(t, err)
		gz := gzip.NewWriter(f)
		_, err = gz.Write(content)
		require.NoError(t, err)
		require.NoError(t, gz.Close())
		require.NoError(t, f.Close())

		data, err := readEMLFile(gzPath)
		require.NoError(t, err, "readEMLFile")
		assert.Equal(t, string(content), string(data))
	})
}

func TestImportSimpleEML(t *testing.T) {
	st := openTestStore(t)

	summary, err := ImportEmlPaths(context.Background(), st,
		[]string{"testdata/simple.eml"},
		EmlImportOptions{
			Identifier: "test@example.com",
			SourceType: "eml",
			NoResume:   true,
		},
	)
	require.NoError(t, err, "ImportEmlPaths")

	assert.Equal(t, int64(1), summary.FilesFound, "FilesFound")
	assert.Equal(t, int64(1), summary.MessagesAdded, "MessagesAdded")
	assert.Equal(t, int64(0), summary.Errors, "Errors")
	assert.Equal(t, 1, countMessages(t, st), "message count")
}

func TestImportSkipsDuplicates(t *testing.T) {
	st := openTestStore(t)

	_, err := ImportEmlPaths(context.Background(), st,
		[]string{"testdata/simple.eml"},
		EmlImportOptions{Identifier: "test@example.com", NoResume: true},
	)
	require.NoError(t, err, "first import")

	// Import again — should skip the duplicate by content hash.
	summary, err := ImportEmlPaths(context.Background(), st,
		[]string{"testdata/simple.eml"},
		EmlImportOptions{Identifier: "test@example.com", NoResume: true},
	)
	require.NoError(t, err, "second import")

	assert.Equal(t, int64(0), summary.MessagesAdded, "MessagesAdded (should skip)")
	assert.Equal(t, int64(1), summary.MessagesSkipped, "MessagesSkipped")
	assert.Equal(t, 1, countMessages(t, st), "message count")
}

func TestImportMultipleFiles(t *testing.T) {
	st := openTestStore(t)

	summary, err := ImportEmlPaths(context.Background(), st,
		[]string{"testdata"},
		EmlImportOptions{Identifier: "test@example.com", NoResume: true},
	)
	require.NoError(t, err, "ImportEmlPaths")

	assert.Equal(t, int64(3), summary.FilesFound, "FilesFound")
	assert.Equal(t, int64(3), summary.MessagesAdded, "MessagesAdded")
	assert.Equal(t, 3, countMessages(t, st), "message count")
}

func TestImportGzippedEML(t *testing.T) {
	st := openTestStore(t)

	// Gzip the simple fixture into a temp .eml.gz file.
	raw, err := os.ReadFile("testdata/simple.eml")
	require.NoError(t, err)
	gzPath := filepath.Join(t.TempDir(), "simple.eml.gz")
	f, err := os.Create(gzPath)
	require.NoError(t, err)
	gz := gzip.NewWriter(f)
	_, err = gz.Write(raw)
	require.NoError(t, err)
	require.NoError(t, gz.Close())
	require.NoError(t, f.Close())

	summary, err := ImportEmlPaths(context.Background(), st,
		[]string{gzPath},
		EmlImportOptions{Identifier: "test@example.com", NoResume: true},
	)
	require.NoError(t, err, "ImportEmlPaths")

	assert.Equal(t, int64(1), summary.FilesFound, "FilesFound")
	assert.Equal(t, int64(1), summary.MessagesAdded, "MessagesAdded")
	assert.Equal(t, 1, countMessages(t, st), "message count")
}
