package tldv

import (
	"fmt"
	"strings"
	"time"
)

// buildBody renders the single body_text shared by FTS and embeddings: title,
// a When line, attendee DISPLAY NAMES, the highlight-derived summary, and the
// transcript. Raw attendee email addresses are deliberately excluded — they
// reach FTS via the toAddrs column only (granola/calsync precedent), including
// the shared "[mm:ss] Speaker: text" transcript-line contract.
func buildBody(m *Meeting, tr *Transcript, n *Notes) string {
	var b strings.Builder
	writeLine := func(s string) {
		if s != "" {
			b.WriteString(s)
			b.WriteString("\n")
		}
	}
	writeLine(meetingTitle(m))
	writeLine(whenLine(m))

	var names []string
	for _, a := range m.Invitees {
		if a.Name != "" {
			names = append(names, a.Name)
		}
	}
	if len(names) > 0 {
		writeLine("Attendees: " + strings.Join(names, ", "))
	}

	if summary := buildSummary(n); summary != "" {
		b.WriteString("\n")
		writeLine(summary)
	}

	if tr != nil && len(tr.Data) > 0 {
		b.WriteString("\nTranscript:\n")
		for _, seg := range tr.Data {
			writeLine(formatTranscriptLine(offsetDuration(seg.StartTime), speakerLabel(seg.Speaker), seg.Text))
		}
	}
	return strings.TrimSpace(b.String())
}

// buildSummary renders the AI notes as the "Summary:" section: the meeting's
// markdown summary when present, else a join of each topic's title and
// summary. Returns "" when there is nothing to summarize.
func buildSummary(n *Notes) string {
	if n == nil {
		return ""
	}
	if md := strings.TrimSpace(n.MarkdownContent); md != "" {
		return "Summary:\n" + md
	}
	var b strings.Builder
	b.WriteString("Summary:\n")
	wroteAny := false
	for _, topic := range n.Topics {
		title := strings.TrimSpace(topic.Title)
		summary := strings.TrimSpace(topic.Summary)
		switch {
		case title != "" && summary != "":
			b.WriteString(title)
			b.WriteString(": ")
			b.WriteString(summary)
			b.WriteString("\n")
			wroteAny = true
		case title != "":
			b.WriteString(title)
			b.WriteString("\n")
			wroteAny = true
		case summary != "":
			b.WriteString(summary)
			b.WriteString("\n")
			wroteAny = true
		}
	}
	if !wroteAny {
		return ""
	}
	return strings.TrimRight(b.String(), "\n")
}

// meetingTitle picks the display/subject title: the meeting name, then a
// date-derived fallback so the row is never blank.
func meetingTitle(m *Meeting) string {
	if m.Name != "" {
		return m.Name
	}
	if !m.HappenedAt.IsZero() {
		return "Meeting on " + m.HappenedAt.UTC().Format("2006-01-02")
	}
	return "Meeting"
}

func whenLine(m *Meeting) string {
	if m.HappenedAt.IsZero() {
		return ""
	}
	return "When: " + m.HappenedAt.UTC().Format("2006-01-02 15:04")
}

// speakerLabel resolves a transcript segment's speaker: the plain display-name
// string, falling back to "Speaker" when empty.
func speakerLabel(speaker string) string {
	if s := strings.TrimSpace(speaker); s != "" {
		return s
	}
	return "Speaker"
}

// offsetDuration converts a float-second offset into a Duration. tl;dv gives
// offsets directly, so no base subtraction is needed.
func offsetDuration(seconds float64) time.Duration {
	if seconds < 0 {
		seconds = 0
	}
	return time.Duration(seconds * float64(time.Second))
}

// formatTranscriptLine renders "[mm:ss] Speaker: text" (or "[h:mm:ss]" past
// the first hour). This line format is the rendering contract shared with the
// granola and circleback importers.
func formatTranscriptLine(offset time.Duration, speaker, text string) string {
	if offset < 0 {
		offset = 0
	}
	total := int(offset.Seconds())
	h, m, s := total/3600, (total%3600)/60, total%60
	stamp := fmt.Sprintf("[%02d:%02d]", m, s)
	if h > 0 {
		stamp = fmt.Sprintf("[%d:%02d:%02d]", h, m, s)
	}
	return stamp + " " + speaker + ": " + text
}

// snippet is a short preview derived from the body.
func snippet(body string) string {
	const maxSnippetLength = 200
	body = strings.TrimSpace(body)
	runes := []rune(body)
	if len(runes) <= maxSnippetLength {
		return body
	}
	return string(runes[:maxSnippetLength])
}
