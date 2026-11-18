"""Thread-safe in-memory storage for an individual node."""

from threading import Lock
from uuid import uuid4

from storage.records import same_record


class ConflictingRevision(Exception):
    """A stale write or conflicting replay was rejected."""


class NodeStorage:
    # Give each process a fresh identity so restarted empty nodes require enrollment.
    def __init__(self):
        self.identity = uuid4().hex
        self.lock = Lock()
        self.entries = {}

    # Return a copy so callers cannot mutate a stored JSON object outside the lock.
    def read(self, key):
        with self.lock:
            record = self.entries.get(key)
            return record.model_copy(deep=True) if record is not None else None

    # Validate the entire batch before publishing any of its records.
    def merge(self, records):
        with self.lock:
            updates = {}
            for record in records:
                previous = updates.get(record.key, self.entries.get(record.key))
                if previous is not None and previous.revision >= record.revision:
                    if same_record(previous, record):
                        continue
                    raise ConflictingRevision(record.key)
                updates[record.key] = record.model_copy(deep=True)
            self.entries.update(updates)

    # Take a stable copy for membership changes and replica repair.
    def records(self):
        with self.lock:
            return [record.model_copy(deep=True) for record in self.entries.values()]
