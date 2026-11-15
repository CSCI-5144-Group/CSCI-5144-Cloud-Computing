"""SQLite helpers and storage for an individual node."""

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4

from storage.records import Record, same_record


# Open a connection per operation and roll back automatically on exception.
@contextmanager
def database(path):
    connection = sqlite3.connect(path, timeout=10)
    connection.row_factory = sqlite3.Row
    try:
        with connection:
            yield connection
    finally:
        connection.close()


# Read and write records with a shared serialization format.
def record_from_row(row):
    return Record(
        key=row["key"], value=json.loads(row["value"]), revision=row["revision"],
        deleted=bool(row["deleted"]),
    )


# Encode a record for SQLite without conflating JSON null with an absent row.
def record_values(record):
    return record.key, json.dumps(record.value, allow_nan=False), record.revision, record.deleted


class ConflictingRevision(Exception):
    """A stale write or conflicting replay was rejected."""


class NodeStorage:
    # Store the node's identity with its data, so an empty volume gets a new identity.
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        with database(path) as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("CREATE TABLE IF NOT EXISTS identity (id TEXT NOT NULL)")
            db.execute("""CREATE TABLE IF NOT EXISTS records (
                key TEXT PRIMARY KEY, value TEXT NOT NULL,
                revision INTEGER NOT NULL, deleted INTEGER NOT NULL
            )""")
            identity = db.execute("SELECT id FROM identity").fetchone()
            self.identity = identity["id"] if identity else uuid4().hex
            if identity is None:
                db.execute("INSERT INTO identity VALUES (?)", (self.identity,))

    # Return the latest local record, including any tombstone.
    def read(self, key):
        with database(self.path) as db:
            row = db.execute("SELECT * FROM records WHERE key = ?", (key,)).fetchone()
        return record_from_row(row) if row else None

    # Serialize writes before checking revisions and applying a batch atomically.
    def merge(self, records):
        with database(self.path) as db:
            db.execute("BEGIN IMMEDIATE")
            for record in records:
                row = db.execute("SELECT * FROM records WHERE key = ?", (record.key,)).fetchone()
                if row and row["revision"] >= record.revision:
                    if same_record(record_from_row(row), record):
                        continue
                    raise ConflictingRevision(record.key)
                db.execute("INSERT OR REPLACE INTO records VALUES (?, ?, ?, ?)",
                           record_values(record))

    # Export one point-in-time view of the node's records.
    def records(self):
        with database(self.path) as db:
            rows = db.execute("SELECT * FROM records ORDER BY key").fetchall()
        return [record_from_row(row) for row in rows]
