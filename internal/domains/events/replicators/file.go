package replicators

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/aromancev/synapse/internal/domains/events"
	"github.com/aromancev/synapse/internal/platform/sqlx"
)

type Replicator interface {
	Name() string
	Replicate(ctx context.Context, event events.Event) error
	Restore(ctx context.Context, repo *events.Repository, db sqlx.DB) error
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

func (r *File) Restore(ctx context.Context, repo *events.Repository, db sqlx.DB) error {
	f, err := os.Open(r.path)
	if err != nil {
		return fmt.Errorf("open replica file: %w", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	var lastGlobalPosition int64
	for line := 1; scanner.Scan(); line++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		var event events.Event
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			return fmt.Errorf("decode replica line %d: %w", line, err)
		}
		if event.GlobalPosition <= 0 {
			return fmt.Errorf("replica line %d: global_position must be > 0", line)
		}
		if lastGlobalPosition > 0 && event.GlobalPosition != lastGlobalPosition+1 {
			return fmt.Errorf("replica line %d: expected global_position %d, got %d", line, lastGlobalPosition+1, event.GlobalPosition)
		}
		lastGlobalPosition = event.GlobalPosition

		if err := repo.AppendEvent(ctx, db, event, event.StreamVersion-1); err != nil {
			return fmt.Errorf("append replica line %d: %w", line, err)
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan replica file: %w", err)
	}

	return nil
}
