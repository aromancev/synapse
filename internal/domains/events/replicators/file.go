package replicators

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/aromancev/synapse/internal/domains/events"
)

type Replicator interface {
	Name() string
	Replicate(ctx context.Context, event events.Event) error
}

type File struct {
	name string
	path string
}

func NewFile(name, path string) *File {
	return &File{name: name, path: path}
}

func (r *File) Name() string {
	return r.name
}

func (r *File) Replicate(ctx context.Context, event events.Event) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if err := os.MkdirAll(filepath.Dir(r.path), 0o755); err != nil {
		return fmt.Errorf("create replica directory: %w", err)
	}

	f, err := os.OpenFile(r.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open replica file: %w", err)
	}
	defer f.Close()

	payload, err := json.Marshal(event.Normalized())
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	if _, err := f.Write(append(payload, '\n')); err != nil {
		return fmt.Errorf("append replica event: %w", err)
	}

	return nil
}
