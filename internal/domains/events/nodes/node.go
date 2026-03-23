package nodes

import (
	"bytes"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aromancev/synapse/internal/domains/events"
	"github.com/oklog/ulid/v2"
)

const idPrefix = "node_"

// ID is a prefixed ULID-backed node identifier.
type ID ulid.ULID

// NewID generates a new node ID.
func NewID() (ID, error) {
	id, err := ulid.New(ulid.Now(), ulid.DefaultEntropy())
	if err != nil {
		return ID{}, fmt.Errorf("generate ulid: %w", err)
	}
	return ID(id), nil
}

func (id ID) String() string {
	return idPrefix + ulid.ULID(id).String()
}

func (id ID) StreamID() events.StreamID {
	return events.StreamID(id.String())
}

func ParseID(s string) (ID, error) {
	if strings.TrimSpace(s) == "" {
		return ID{}, errors.New("id is required")
	}
	if !strings.HasPrefix(s, idPrefix) {
		return ID{}, fmt.Errorf("id must have prefix %q", idPrefix)
	}
	parsed, err := ulid.ParseStrict(strings.TrimPrefix(s, idPrefix))
	if err != nil {
		return ID{}, fmt.Errorf("parse ulid: %w", err)
	}
	return ID(parsed), nil
}

func (id *ID) Scan(src any) error {
	v, ok := src.([]byte)
	if !ok {
		return fmt.Errorf("cannot scan %T into ID", src)
	}
	if len(v) != len(ulid.ULID{}) {
		return fmt.Errorf("invalid byte length for ID: %d", len(v))
	}
	var parsed ulid.ULID
	copy(parsed[:], v)
	*id = ID(parsed)
	return nil
}

func (id ID) Value() (driver.Value, error) {
	value := ulid.ULID(id)
	return value[:], nil
}

func (id ID) MarshalJSON() ([]byte, error) {
	return []byte(`"` + id.String() + `"`), nil
}

func (id *ID) UnmarshalJSON(data []byte) error {
	parsed, err := ParseID(strings.Trim(string(data), `"`))
	if err != nil {
		return err
	}
	*id = parsed
	return nil
}

// Node is a stored payload bound to a schema stream.
type Node struct {
	ID         ID              `json:"id"`
	SchemaID   events.StreamID `json:"schema_id"`
	CreatedAt  int64           `json:"created_at"`
	ArchivedAt int64           `json:"archived_at"`
	Payload    json.RawMessage `json:"payload"`
	Keywords   []string        `json:"keywords"`
	SearchText string          `json:"search_text"`
}

// Normalized returns a normalized copy of the node.
func (n Node) Normalized() Node {
	n.SchemaID = n.SchemaID.Normalized()
	n.Payload = json.RawMessage(strings.TrimSpace(string(n.Payload)))

	var compact bytes.Buffer
	if err := json.Compact(&compact, []byte(n.Payload)); err == nil {
		n.Payload = json.RawMessage(compact.Bytes())
	}

	n.Keywords = NormalizeKeywords(n.Keywords)
	n.SearchText = strings.Join(strings.Fields(strings.TrimSpace(n.SearchText)), " ")
	if n.SearchText == "" && len(n.Payload) > 0 {
		n.SearchText = BuildSearchText(n.Payload, n.Keywords)
	}

	return n
}

// Validate checks whether Node is valid.
func (n Node) Validate() []error {
	var errs []error

	var emptyID ID
	if n.ID == emptyID {
		errs = append(errs, errors.New("id is required"))
	}

	if err := n.SchemaID.Validate(); err != nil {
		errs = append(errs, fmt.Errorf("schema_id: %w", err))
	}

	if n.CreatedAt <= 0 {
		errs = append(errs, errors.New("created_at is required"))
	} else if n.CreatedAt > time.Now().Add(24*time.Hour).Unix() {
		errs = append(errs, errors.New("created_at cannot be in the far future"))
	}

	if n.ArchivedAt < 0 {
		errs = append(errs, errors.New("archived_at cannot be negative"))
	} else if n.ArchivedAt > 0 {
		if n.ArchivedAt < n.CreatedAt {
			errs = append(errs, errors.New("archived_at cannot be before created_at"))
		}
		if n.ArchivedAt > time.Now().Add(24*time.Hour).Unix() {
			errs = append(errs, errors.New("archived_at cannot be in the far future"))
		}
	}

	if len(n.Payload) == 0 {
		errs = append(errs, errors.New("payload is required"))
	} else {
		if len(n.Payload) > 256*1024 {
			errs = append(errs, errors.New("payload must not exceed 256KB"))
		}
		var doc any
		if err := json.Unmarshal(n.Payload, &doc); err != nil {
			errs = append(errs, errors.New("payload must be valid JSON"))
		}
	}

	if len(n.Keywords) > 64 {
		errs = append(errs, errors.New("keywords must not exceed 64 items"))
	}
	for _, keyword := range n.Keywords {
		if keyword == "" {
			errs = append(errs, errors.New("keywords must not contain empty values"))
			continue
		}
		if len(keyword) > 64 {
			errs = append(errs, errors.New("keyword must not exceed 64 characters"))
		}
	}

	return errs
}

func NormalizeKeywords(keywords []string) []string {
	if len(keywords) == 0 {
		return nil
	}

	seen := make(map[string]struct{}, len(keywords))
	out := make([]string, 0, len(keywords))
	for _, keyword := range keywords {
		keyword = strings.ToLower(strings.TrimSpace(keyword))
		keyword = strings.Join(strings.Fields(keyword), " ")
		if keyword == "" {
			continue
		}
		if _, ok := seen[keyword]; ok {
			continue
		}
		seen[keyword] = struct{}{}
		out = append(out, keyword)
	}

	return out
}
