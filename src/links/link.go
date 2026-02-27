package links

import (
	"errors"
	"time"
)

// Link represents a directional edge between two nodes.
type Link struct {
	From      int64   `json:"from"`
	To        int64   `json:"to"`
	Weight    float64 `json:"weight"`
	CreatedAt int64   `json:"created_at"`
}

// Validate checks whether Link is valid.
func (l Link) Validate() []error {
	var errs []error

	if l.From <= 0 {
		errs = append(errs, errors.New("from is required and must be > 0"))
	}
	if l.To <= 0 {
		errs = append(errs, errors.New("to is required and must be > 0"))
	}
	if l.From == l.To {
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
