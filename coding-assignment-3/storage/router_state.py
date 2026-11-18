"""Keep membership, revisions, and unfinished operations in process memory."""

from copy import deepcopy
from threading import Lock

from storage.records import Record


class RouterState:
    # All router state lasts for the lifetime of this single process.
    def __init__(self):
        self.lock = Lock()
        self.settings = {}
        self.clock = 0
        self.pending = {}

    # Copy routing metadata so callers cannot change the stored membership.
    def get_setting(self, key):
        with self.lock:
            return deepcopy(self.settings.get(key))

    # Keep membership updates separate from the KV request path.
    def set_setting(self, key, value):
        with self.lock:
            self.settings[key] = deepcopy(value)

    # Allocate a unique revision and remember the mutation before sending it.
    def begin_write(self, key, value, deleted):
        with self.lock:
            if key in self.pending:
                raise ValueError("A mutation for this key is already pending")
            revision = self.clock + 1
            record = Record(key=key, value=deepcopy(value), revision=revision, deleted=deleted)
            self.clock = revision
            self.pending[key] = record
        return record

    # Check for an unfinished mutation before serving another operation on its key.
    def pending_for(self, key):
        with self.lock:
            return self.pending.get(key)

    # Copy the queue while other worker threads handle independent keys.
    def pending_records(self):
        with self.lock:
            return sorted(self.pending.values(), key=lambda record: record.revision)

    # Report unfinished mutations without scanning their values.
    def pending_count(self):
        with self.lock:
            return len(self.pending)

    # Remove a mutation only if it is the revision acknowledged by every owner.
    def finish_write(self, record):
        with self.lock:
            current = self.pending.get(record.key)
            if current is not None and current.revision == record.revision:
                del self.pending[record.key]
