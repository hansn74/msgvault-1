package slack

import (
	"archive/zip"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/msgvault/internal/testutil"
)

// ts strings used across the fixture and assertions.
const (
	tsGen1 = "1577836801.000000" // UALICE, mentions UBOB + link
	tsGen2 = "1577836802.000000" // UBOB, thread root, reacted
	tsGen3 = "1577836803.000000" // UALICE, thread reply -> tsGen2
	tsGen4 = "1577836804.000000" // bot message
	tsSec1 = "1577836810.000000" // UME (is_from_me) in private channel
	tsMp1  = "1577836820.000000" // UBOB in group DM
	tsDm1  = "1577836830.000000" // UALICE in DM (to = UME)
)

// writeExportFixture lays out a minimal but representative official Slack
// export under root: catalogs + one folder per conversation of per-day JSON.
func writeExportFixture(t *testing.T, root string) {
	t.Helper()
	write := func(rel, body string) {
		p := filepath.Join(root, rel)
		require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o755))
		require.NoError(t, os.WriteFile(p, []byte(body), 0o644))
	}

	write("users.json", `[
	  {"id":"UALICE","team_id":"T1","name":"alice","real_name":"Alice","profile":{"email":"alice@example.com","display_name":"Alice","real_name":"Alice"}},
	  {"id":"UBOB","team_id":"T1","name":"bob","profile":{"email":"bob@example.com","display_name":"Bob"}},
	  {"id":"UME","team_id":"T1","name":"me","profile":{"email":"me@example.com","display_name":"Me"}}
	]`)
	write("channels.json", `[{"id":"CGEN","name":"general","members":["UALICE","UBOB","UME"]}]`)
	write("groups.json", `[{"id":"GSEC","name":"secrets","members":["UALICE","UME"]}]`)
	write("mpims.json", `[{"id":"GMP","name":"mpdm-alice--bob--me-1","members":["UALICE","UBOB","UME"]}]`)
	write("dms.json", `[{"id":"DAB","members":["UALICE","UME"]}]`)

	// Public channel: mention+link, a thread (root reacted, reply), a bot msg.
	write("general/2020-01-01.json", `[
	  {"type":"message","ts":"`+tsGen1+`","user":"UALICE","text":"hello <@UBOB> and <http://x.com|link>"},
	  {"type":"message","ts":"`+tsGen2+`","user":"UBOB","text":"root","thread_ts":"`+tsGen2+`","reply_count":1,"reactions":[{"name":"thumbsup","users":["UME"],"count":1}]},
	  {"type":"message","ts":"`+tsGen3+`","user":"UALICE","text":"a reply","thread_ts":"`+tsGen2+`"},
	  {"type":"message","subtype":"bot_message","ts":"`+tsGen4+`","bot_id":"B1","username":"GitHub","text":"","attachments":[{"fallback":"PR opened: fix things"}]}
	]`)
	// Private channel folder is named by channel name too.
	write("secrets/2020-01-01.json", `[{"type":"message","ts":"`+tsSec1+`","user":"UME","text":"secret note"}]`)
	// Group DM folder by name.
	write("mpdm-alice--bob--me-1/2020-01-01.json", `[{"type":"message","ts":"`+tsMp1+`","user":"UBOB","text":"group hi"}]`)
	// DM folder is named by ID.
	write("DAB/2020-01-01.json", `[{"type":"message","ts":"`+tsDm1+`","user":"UALICE","text":"dm hi"}]`)
}

func TestImportExport_Directory(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	root := t.TempDir()
	writeExportFixture(t, root)

	st := testutil.NewTestStore(t)
	imp := NewImporter(st, nil, "T1") // nil client: export path makes no API calls
	sum, err := imp.ImportExport(context.Background(), root, ExportOptions{UserID: "UME"})
	require.NoError(err)

	assert.Equal(4, sum.ConversationsProcessed)
	assert.Equal(7, sum.MessagesProcessed)
	assert.Equal(7, sum.MessagesAdded)
	assert.Zero(sum.MessagesUpdated)
	assert.NotZero(sum.SourceID)

	// Total Slack messages persisted.
	var msgCount int
	require.NoError(st.DB().QueryRow(`SELECT COUNT(*) FROM messages WHERE message_type='slack'`).Scan(&msgCount))
	assert.Equal(7, msgCount)

	// Conversation types/titles.
	type ct struct{ title, ctype string }
	get := func(sourceConvID string) ct {
		var c ct
		require.NoError(st.DB().QueryRow(st.Rebind(
			`SELECT title, conversation_type FROM conversations WHERE source_conversation_id = ?`), sourceConvID).
			Scan(&c.title, &c.ctype))
		return c
	}
	assert.Equal(ct{"#general", "channel"}, get("CGEN"))
	assert.Equal(ct{"#secrets", "channel"}, get("GSEC"))
	assert.Equal("group_chat", get("GMP").ctype)
	assert.Equal(ct{"Alice", "direct_chat"}, get("DAB"), "DM titled by peer display name")

	// Thread reply is linked to its root by source_message_id.
	var linked int
	require.NoError(st.DB().QueryRow(st.Rebind(`
		SELECT COUNT(*) FROM messages child
		JOIN messages parent ON child.reply_to_message_id = parent.id
		WHERE child.source_message_id = ? AND parent.source_message_id = ?`),
		"CGEN:"+tsGen3, "CGEN:"+tsGen2).Scan(&linked))
	assert.Equal(1, linked, "thread reply links to root")

	// Reaction on the root.
	var reactions int
	require.NoError(st.DB().QueryRow(st.Rebind(`
		SELECT COUNT(*) FROM reactions r JOIN messages m ON m.id = r.message_id
		WHERE m.source_message_id = ? AND r.reaction_value = 'thumbsup'`),
		"CGEN:"+tsGen2).Scan(&reactions))
	assert.Equal(1, reactions)

	// Mention recipient on the first message.
	var mentions int
	require.NoError(st.DB().QueryRow(st.Rebind(`
		SELECT COUNT(*) FROM message_recipients mr JOIN messages m ON m.id = mr.message_id
		WHERE m.source_message_id = ? AND mr.recipient_type = 'mention'`),
		"CGEN:"+tsGen1).Scan(&mentions))
	assert.Equal(1, mentions)

	// Body renders mrkdwn: mention -> @Bob, link -> "label (url)".
	var body string
	require.NoError(st.DB().QueryRow(st.Rebind(`
		SELECT mb.body_text FROM message_bodies mb JOIN messages m ON m.id = mb.message_id
		WHERE m.source_message_id = ?`), "CGEN:"+tsGen1).Scan(&body))
	assert.Contains(body, "@Bob")
	assert.Contains(body, "link (http://x.com)")

	// Bot message with empty text falls back to attachment fallback text.
	var botBody string
	require.NoError(st.DB().QueryRow(st.Rebind(`
		SELECT mb.body_text FROM message_bodies mb JOIN messages m ON m.id = mb.message_id
		WHERE m.source_message_id = ?`), "CGEN:"+tsGen4).Scan(&botBody))
	assert.Contains(botBody, "PR opened: fix things")

	// is_from_me set for the archiving user's own message.
	var fromMe bool
	require.NoError(st.DB().QueryRow(st.Rebind(
		`SELECT is_from_me FROM messages WHERE source_message_id = ?`), "GSEC:"+tsSec1).Scan(&fromMe))
	assert.True(fromMe)

	// DM "to" recipient is the archiving user (the non-sender member).
	var dmTo int
	require.NoError(st.DB().QueryRow(st.Rebind(`
		SELECT COUNT(*) FROM message_recipients mr JOIN messages m ON m.id = mr.message_id
		WHERE m.source_message_id = ? AND mr.recipient_type = 'to'`),
		"DAB:"+tsDm1).Scan(&dmTo))
	assert.Equal(1, dmTo, "DM message has one 'to' recipient (me)")

	// Every message got a 'from' recipient and archived raw JSON.
	var fromRows, rawRows int
	require.NoError(st.DB().QueryRow(
		`SELECT COUNT(*) FROM message_recipients WHERE recipient_type='from'`).Scan(&fromRows))
	assert.Equal(7, fromRows)
	require.NoError(st.DB().QueryRow(st.Rebind(
		`SELECT COUNT(*) FROM message_raw mr JOIN messages m ON m.id = mr.message_id
		 WHERE m.message_type='slack' AND mr.raw_format = 'slack_json'`)).Scan(&rawRows))
	assert.Equal(7, rawRows)

	// Re-import is idempotent: same source, everything updated in place.
	sum2, err := imp.ImportExport(context.Background(), root, ExportOptions{UserID: "UME"})
	require.NoError(err)
	assert.Equal(sum.SourceID, sum2.SourceID, "same source row on re-import")
	assert.Equal(7, sum2.MessagesProcessed)
	assert.Zero(sum2.MessagesAdded, "no new messages on re-import")
	assert.Equal(7, sum2.MessagesUpdated, "all deduped/updated in place")

	require.NoError(st.DB().QueryRow(`SELECT COUNT(*) FROM messages WHERE message_type='slack'`).Scan(&msgCount))
	assert.Equal(7, msgCount, "no duplicate rows after re-import")
}

func TestImportExport_Zip(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	root := t.TempDir()
	writeExportFixture(t, root)
	zipPath := filepath.Join(t.TempDir(), "export.zip")
	zipDir(t, root, zipPath)

	st := testutil.NewTestStore(t)
	imp := NewImporter(st, nil, "T1")
	sum, err := imp.ImportExport(context.Background(), zipPath, ExportOptions{UserID: "UME"})
	require.NoError(err)
	assert.Equal(4, sum.ConversationsProcessed)
	assert.Equal(7, sum.MessagesProcessed)

	var msgCount int
	require.NoError(st.DB().QueryRow(`SELECT COUNT(*) FROM messages WHERE message_type='slack'`).Scan(&msgCount))
	assert.Equal(7, msgCount)
}

func TestPeekExportTeamID(t *testing.T) {
	root := t.TempDir()
	writeExportFixture(t, root)
	team, err := PeekExportTeamID(root)
	require.NoError(t, err)
	assert.Equal(t, "T1", team)
}

// zipDir writes every file under root into a zip at dest, using paths relative
// to root (the Slack export layout: no wrapping directory).
func zipDir(t *testing.T, root, dest string) {
	t.Helper()
	f, err := os.Create(dest)
	require.NoError(t, err)
	defer func() { require.NoError(t, f.Close()) }()
	zw := zip.NewWriter(f)
	require.NoError(t, filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		w, err := zw.Create(filepath.ToSlash(rel))
		if err != nil {
			return err
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		_, err = w.Write(b)
		return err
	}))
	require.NoError(t, zw.Close())
}

// An export re-import must honour the same [slack] channel filters as the
// live sync; otherwise re-importing would reinstate exactly the noisy
// channels the operator excluded from syncing. DMs and group DMs stay
// unfiltered — the filters exist to skip channels, not people.
func TestImportExport_ExcludeChannels(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	root := t.TempDir()
	writeExportFixture(t, root)

	st := testutil.NewTestStore(t)
	imp := NewImporter(st, nil, "T1")
	sum, err := imp.ImportExport(context.Background(), root, ExportOptions{
		UserID:          "UME",
		ExcludeChannels: []string{"general"},
	})
	require.NoError(err)
	assert.Equal(1, sum.ConversationsSkipped, "#general is excluded")
	assert.Equal(3, sum.ConversationsProcessed, "secrets, group DM and DM still imported")

	var generalRows int
	require.NoError(st.DB().QueryRow(st.Rebind(
		`SELECT COUNT(*) FROM conversations WHERE source_conversation_id = ?`), "CGEN").Scan(&generalRows))
	assert.Zero(generalRows, "no conversation row is created for an excluded channel")

	var msgs int
	require.NoError(st.DB().QueryRow(`SELECT COUNT(*) FROM messages WHERE message_type='slack'`).Scan(&msgs))
	assert.Equal(3, msgs, "only the private channel, group DM and DM messages remain")

	// The DM and group DM are untouched by a channel-name filter.
	for _, id := range []string{"DAB", "GMP"} {
		var n int
		require.NoError(st.DB().QueryRow(st.Rebind(
			`SELECT COUNT(*) FROM conversations WHERE source_conversation_id = ?`), id).Scan(&n))
		assert.Equal(1, n, "conversation %s must survive a channel filter", id)
	}
}
