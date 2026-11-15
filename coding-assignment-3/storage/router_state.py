"""Persist membership, the revision clock, and unfinished operations."""

import json
from pathlib import Path

from storage.records import Record
from storage.sqlite_store import database, record_from_row, record_values


class RouterState:
    # Keep routing metadata and the mutation journal on the router's own volume.
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        with database(path) as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)")
            db.execute("INSERT OR IGNORE INTO settings VALUES ('clock', '0')")
            db.execute("""CREATE TABLE IF NOT EXISTS pending (
                key TEXT PRIMARY KEY, value TEXT NOT NULL,
                revision INTEGER NOT NULL, deleted INTEGER NOT NULL
            )""")

    # Load either the durable node identities or the in-progress restore payload.
    def get_setting(self, key):
        with database(self.path) as db:
            row = db.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
        return json.loads(row["value"]) if row else None

    # Store a setting without exposing its JSON encoding to the router service.
    def set_setting(self, key, value):
        with database(self.path) as db:
            db.execute("INSERT OR REPLACE INTO settings VALUES (?, ?)", (key, json.dumps(value)))

    # Allocate a revision and journal its operation in the same transaction.
    def begin_write(self, key, value, deleted):
        with database(self.path) as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT value FROM settings WHERE key = 'clock'").fetchone()
            revision = int(row["value"]) + 1
            record = Record(key=key, value=value, revision=revision, deleted=deleted)
            db.execute("UPDATE settings SET value = ? WHERE key = 'clock'", (str(revision),))
            db.execute("INSERT INTO pending VALUES (?, ?, ?, ?)", record_values(record))
        return record

    # Read a pending entry to prevent another request from bypassing it.
    def pending_for(self, key):
        with database(self.path) as db:
            row = db.execute("SELECT * FROM pending WHERE key = ?", (key,)).fetchone()
        return record_from_row(row) if row else None

    # Take a snapshot of the queue for the retry worker.
    def pending_records(self):
        with database(self.path) as db:
            rows = db.execute("SELECT * FROM pending ORDER BY revision").fetchall()
        return [record_from_row(row) for row in rows]

    # Count journal entries without loading their values for status reports.
    def pending_count(self):
        with database(self.path) as db:
            return db.execute("SELECT COUNT(*) FROM pending").fetchone()[0]

    # Discard the journal entry only after every enrolled owner acknowledges it.
    def finish_write(self, record):
        with database(self.path) as db:
            db.execute("DELETE FROM pending WHERE key = ? AND revision = ?",
                       (record.key, record.revision))

    # Publish the restored revision clock and reopen the router atomically.
    def finish_restore(self, clock):
        with database(self.path) as db:
            db.execute("UPDATE settings SET value = ? WHERE key = 'clock'", (str(clock),))
            db.execute("DELETE FROM settings WHERE key = 'restore'")
