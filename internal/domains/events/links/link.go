package links

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aromancev/synapse/internal/domains/events"
)

// Link is the projection model for links.
type Link struct {
	From      events.StreamID `json:"from"`
	To        events.StreamID `json:"to"`
	Weight    float64         `json:"weight"`
	CreatedAt int64           `json:"created_at"`
}

// Validate checks whether Link is valid.
func (l Link) Validate() []error {
	var errs []error

	if err := l.From.Validate(); err != nil {
		errs = append(errs, fmt.Errorf("from: %w", err))
	}
	if err := l.To.Validate(); err != nil {
		errs = append(errs, fmt.Errorf("to: %w", err))
	}
	if l.From.Normalized() == l.To.Normalized() {
		errs = append(errs, errors.New("from and to must be different"))
	}
	if l.Weight < 0 || l.Weight > 1 {
		errs = append(errs, errors.New("weight must be between 0 and 1"))
	}
	if l.CreatedAt <= 0 {
		errs = append(errs, errors.New("created_at is required"))
	} else if l.CreatedAt > time.Now().Unix() {
		errs = append(errs, errors.New("created_at cannot be in the future"))
	}

	return errs
}

func StreamIDForPair(from, to events.StreamID) events.StreamID {
	from, to = normalizePair(from, to)
	return events.StreamID(fmt.Sprintf("link_%s_%s", from, to))
}

func normalizePair(from, to events.StreamID) (events.StreamID, events.StreamID) {
	from = from.Normalized()
	to = to.Normalized()
	if strings.Compare(from.String(), to.String()) > 0 {
		from, to = to, from
	}
	return from, to
}
