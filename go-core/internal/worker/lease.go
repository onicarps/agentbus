package worker

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"time"

	"github.com/google/uuid"
)

// LeaseStore is a minimal local advisory lease table in events.db (Python parity).
type LeaseStore struct {
	db        *sql.DB
	workspace string
}

func NewLeaseStore(db *sql.DB, workspace string) (*LeaseStore, error) {
	ls := &LeaseStore{db: db, workspace: workspace}
	_, err := db.Exec(`
CREATE TABLE IF NOT EXISTS leases (
  lease_id TEXT PRIMARY KEY,
  resource TEXT NOT NULL UNIQUE,
  owner_id TEXT NOT NULL,
  acquired_at TEXT NOT NULL,
  expires_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_leases_resource ON leases(resource);
CREATE INDEX IF NOT EXISTS idx_leases_expires ON leases(expires_at);
`)
	return ls, err
}

func (ls *LeaseStore) resourcePath(topic string, eventID int64) string {
	// Path under workspace so it mirrors Python normalize_resource semantics.
	return filepath.Join(ls.workspace, ".agentbus", "wake-locks", topic, fmt.Sprintf("%d", eventID))
}

func fmtUTC(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05Z")
}

func (ls *LeaseStore) purge(now time.Time) {
	_, _ = ls.db.Exec(`DELETE FROM leases WHERE expires_at <= ?`, fmtUTC(now))
}

// TryAcquire returns true if this owner holds the lease for the event.
func (ls *LeaseStore) TryAcquire(topic string, eventID int64, owner string, ttlSec int) (bool, string, error) {
	if ttlSec <= 0 {
		ttlSec = 300
	}
	now := time.Now().UTC()
	ls.purge(now)
	res := ls.resourcePath(topic, eventID)
	var curOwner, leaseID, exp string
	err := ls.db.QueryRow(
		`SELECT owner_id, lease_id, expires_at FROM leases WHERE resource = ?`, res,
	).Scan(&curOwner, &leaseID, &exp)
	if err == nil {
		if curOwner == owner {
			return true, leaseID, nil
		}
		return false, "", nil
	}
	if err != sql.ErrNoRows {
		return false, "", err
	}
	id := uuid.NewString()
	expires := now.Add(time.Duration(ttlSec) * time.Second)
	_, err = ls.db.Exec(
		`INSERT INTO leases (lease_id, resource, owner_id, acquired_at, expires_at) VALUES (?,?,?,?,?)`,
		id, res, owner, fmtUTC(now), fmtUTC(expires),
	)
	if err != nil {
		// race: another worker won
		return false, "", nil
	}
	return true, id, nil
}

func (ls *LeaseStore) Release(topic string, eventID int64, owner, leaseID string) {
	res := ls.resourcePath(topic, eventID)
	_, _ = ls.db.Exec(
		`DELETE FROM leases WHERE resource = ? AND lease_id = ? AND owner_id = ?`,
		res, leaseID, owner,
	)
}
