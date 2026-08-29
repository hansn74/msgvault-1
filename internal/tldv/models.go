// Package tldv imports meeting recordings and transcripts from the tl;dv
// public REST API (https://pasta.tldv.io) into the msgvault store. Each meeting
// becomes one conversation of type "meeting" holding a single
// "meeting_transcript" message whose body carries the highlight summary and
// the full transcript.
//
// Unlike Granola, tl;dv meetings are immutable once recorded — they carry no
// updated_at and are never re-edited server-side — so the incremental cursor
// is a simple watermark over the meeting happenedAt timestamp.
package tldv

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const (
	// SourceType is the sources.source_type value for tl;dv accounts.
	SourceType = "tldv"
	// MessageType is the messages.message_type value for meeting transcripts.
	MessageType = "meeting_transcript"
	// ConversationType is the conversations.conversation_type value.
	ConversationType = "meeting"
	// RawFormat tags message_raw rows holding the verbatim API responses.
	RawFormat = "tldv_json"
	// DefaultBaseURL is the production API host.
	DefaultBaseURL = "https://pasta.tldv.io"
)

// apiTimestamp is a tolerant timestamp decoder. The live API returns
// JavaScript Date.toString() values ("Fri Aug 28 2026 12:30:00 GMT+0000
// (Coordinated Universal Time)") despite the docs implying ISO8601, so it
// accepts that form plus RFC3339Nano and date-only ("2006-01-02"), and treats
// null/empty as the zero time so absent timestamps never fail a whole meeting.
type apiTimestamp time.Time

// jsDateLayout matches JavaScript Date.toString() output once the trailing
// parenthesized zone name has been stripped.
const jsDateLayout = "Mon Jan 02 2006 15:04:05 GMT-0700"

func (t *apiTimestamp) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		*t = apiTimestamp(time.Time{})
		return nil
	}
	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return fmt.Errorf("decode tl;dv timestamp: %w", err)
	}
	if value == "" {
		*t = apiTimestamp(time.Time{})
		return nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		parsed, err = time.Parse(time.DateOnly, value)
	}
	if err != nil {
		// JS Date.toString(): drop the " (Zone Name)" suffix before parsing.
		trimmed := value
		if i := strings.Index(trimmed, " ("); i > 0 {
			trimmed = trimmed[:i]
		}
		parsed, err = time.Parse(jsDateLayout, trimmed)
	}
	if err != nil {
		return fmt.Errorf("parse tl;dv timestamp %q: %w", value, err)
	}
	*t = apiTimestamp(parsed)
	return nil
}

// Person is a meeting organizer or invitee. Email is normally present; name
// may be empty.
type Person struct {
	Name  string `json:"name"`
	Email string `json:"email"`
}

// Meeting is the object returned by both the list and detail endpoints.
// HappenedAt is an absolute timestamp. Raw preserves the verbatim detail
// response body for archival in message_raw.
type Meeting struct {
	ID         string    `json:"id"`
	Name       string    `json:"name"`
	HappenedAt time.Time `json:"happenedAt"`
	URL        string    `json:"url"`
	Organizer  Person    `json:"organizer"`
	Invitees   []Person  `json:"invitees"`

	// Duration is the meeting length in seconds. Template is the name of the
	// notes template applied to the meeting (a plain string, not an object).
	Duration        float64 `json:"duration"`
	Template        string  `json:"template"`
	ExtraProperties struct {
		ConferenceID string `json:"conferenceId"`
	} `json:"extraProperties"`

	Raw json.RawMessage `json:"-"`
}

func (m *Meeting) UnmarshalJSON(data []byte) error {
	type plain Meeting
	decoded := struct {
		*plain

		HappenedAt apiTimestamp `json:"happenedAt"`
	}{plain: (*plain)(m)}
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	m.HappenedAt = time.Time(decoded.HappenedAt)
	return nil
}

// ListMeetingsOutput is the GET /v1alpha1/meetings response envelope.
// Pagination is page-number based: pages run 1..Pages.
type ListMeetingsOutput struct {
	Page     int       `json:"page"`
	Pages    int       `json:"pages"`
	Total    int       `json:"total"`
	PageSize int       `json:"pageSize"`
	Results  []Meeting `json:"results"`
}

// TranscriptSegment is one utterance. Speaker is a plain display-name string.
// StartTime/EndTime are float SECONDS offsets from the meeting start, not
// absolute timestamps.
type TranscriptSegment struct {
	Speaker   string  `json:"speaker"`
	Text      string  `json:"text"`
	StartTime float64 `json:"startTime"`
	EndTime   float64 `json:"endTime"`
}

// Transcript is the GET /v1alpha1/meetings/{id}/transcript response. Raw
// preserves the verbatim response body.
type Transcript struct {
	ID        string              `json:"id"`
	MeetingID string              `json:"meetingId"`
	Data      []TranscriptSegment `json:"data"`

	Raw json.RawMessage `json:"-"`
}

// StructuredNote is one AI-generated note entry. Timestamp is a float seconds
// offset from the meeting start; TopicID links the note to a NotesTopic.
type StructuredNote struct {
	SegmentID string  `json:"segmentId"`
	Timestamp float64 `json:"timestamp"`
	Text      string  `json:"text"`
	TopicID   string  `json:"topicId"`
}

// NotesTopic groups structured notes under a title with a per-topic summary.
type NotesTopic struct {
	ID      string `json:"id"`
	Order   int    `json:"order"`
	Title   string `json:"title"`
	Summary string `json:"summary"`
}

// Notes is the GET /v1alpha1/meetings/{id}/notes response — the AI summary
// that supersedes the deprecated /highlights endpoint. The endpoint is
// optional: a 404 or error yields a nil Notes that the importer treats as
// absent. Raw preserves the verbatim response body.
type Notes struct {
	StructuredNotes []StructuredNote `json:"structuredNotes"`
	MarkdownContent string           `json:"markdownContent"`
	Topics          []NotesTopic     `json:"topics"`

	Raw json.RawMessage `json:"-"`
}
