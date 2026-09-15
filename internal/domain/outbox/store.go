package outbox

import (
	"context"
	"encoding/json"
	"time"

	"github.com/pkg/errors"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const defaultClaimStale = 2 * time.Minute

type Store struct {
	db    *gorm.DB
	stale time.Duration
}

func NewStore(db *gorm.DB) *Store {
	if db == nil {
		panic("outbox store: gorm db is nil")
	}
	return &Store{db: db, stale: defaultClaimStale}
}

func (s *Store) DB() *gorm.DB {
	return s.db
}

// Insert writes a pending event. Pass tx to join a business transaction.
func (s *Store) Insert(ctx context.Context, tx *gorm.DB, ev *Event) error {
	db := s.db.WithContext(ctx)
	if tx != nil {
		db = tx.WithContext(ctx)
	}
	if err := db.Create(ev).Error; err != nil {
		return errors.Wrap(err, "insert outbox event")
	}
	return nil
}

// Claim locks a batch of pending (or stale publishing) rows and marks them publishing.
func (s *Store) Claim(ctx context.Context, limit int) ([]Event, error) {
	if limit <= 0 {
		limit = 50
	}
	cutoff := time.Now().Add(-s.stale)
	var events []Event

	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		q := tx.Where(
			"status = ? OR (status = ? AND updated_at < ?)",
			StatusPending, StatusPublishing, cutoff,
		).Order("created_at ASC").Limit(limit)
		if tx.Dialector.Name() == "postgres" {
			q = q.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"})
		}
		if err := q.Find(&events).Error; err != nil {
			return err
		}
		if len(events) == 0 {
			return nil
		}
		ids := make([]string, len(events))
		for i, ev := range events {
			ids[i] = ev.ID
		}
		now := time.Now()
		if err := tx.Model(&Event{}).Where("id IN ?", ids).Updates(map[string]any{
			"status":     StatusPublishing,
			"updated_at": now,
		}).Error; err != nil {
			return err
		}
		for i := range events {
			events[i].Status = StatusPublishing
			events[i].UpdatedAt = now
		}
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "claim outbox events")
	}
	return events, nil
}

func (s *Store) MarkPublished(ctx context.Context, id string) error {
	now := time.Now()
	res := s.db.WithContext(ctx).Model(&Event{}).Where("id = ?", id).Updates(map[string]any{
		"status":       StatusPublished,
		"error":        nil,
		"published_at": now,
		"updated_at":   now,
	})
	if res.Error != nil {
		return errors.Wrap(res.Error, "mark outbox published")
	}
	if res.RowsAffected == 0 {
		return errors.Errorf("outbox event %s not found", id)
	}
	return nil
}

func (s *Store) MarkFailed(ctx context.Context, id string, attempts int, publishErr error, giveUp bool) error {
	status := StatusPending
	if giveUp {
		status = StatusFailed
	}
	body, _ := json.Marshal(map[string]string{"message": publishErr.Error()})
	now := time.Now()
	res := s.db.WithContext(ctx).Model(&Event{}).Where("id = ?", id).Updates(map[string]any{
		"status":     status,
		"attempts":   attempts,
		"error":      json.RawMessage(body),
		"updated_at": now,
	})
	if res.Error != nil {
		return errors.Wrap(res.Error, "mark outbox failed")
	}
	if res.RowsAffected == 0 {
		return errors.Errorf("outbox event %s not found", id)
	}
	return nil
}
