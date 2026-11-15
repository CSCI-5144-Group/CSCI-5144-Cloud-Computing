"""Coordinate replicated reads and writes for a single router process."""

import logging
from contextlib import contextmanager
from threading import Condition, Lock

from routing.hash_ring import HashRing, hash_value
from routing.node_client import Node, NodeUnavailable

logger = logging.getLogger("uvicorn.error")


class ServiceError(Exception):
    def __init__(self, status, message):
        super().__init__(message)
        self.status = status


class RequestGate:
    # Protect membership changes while allowing requests for different keys to overlap.
    def __init__(self):
        self.condition = Condition()
        self.active = 0
        self.maintenance = False

    # Let current traffic drain and reject new work while maintenance is active.
    @contextmanager
    def exclusive(self):
        with self.condition:
            if self.maintenance:
                raise ServiceError(503, "Another maintenance operation is running")
            self.maintenance = True
            self.condition.wait_for(lambda: self.active == 0)
        try:
            yield
        finally:
            with self.condition:
                self.maintenance = False

    # Track active work until the request finishes or raises an error.
    @contextmanager
    def request(self):
        with self.condition:
            if self.maintenance:
                raise ServiceError(503, "Maintenance in progress")
            self.active += 1
        try:
            yield
        finally:
            with self.condition:
                self.active -= 1
                if self.active == 0:
                    self.condition.notify_all()


class KVService:
    # Start with persisted membership when the router process restarts.
    def __init__(self, state, client, urls):
        self.state = state
        self.client = client
        self.urls = urls
        self.nodes = {}
        self.ring = None
        self.gate = RequestGate()
        self.key_locks = [Lock() for _ in range(256)]
        self.restoring = self.state.get_setting("restore") is not None
        saved = self.state.get_setting("membership")
        if saved is not None:
            unknown = set(saved) - self.urls.keys()
            if unknown:
                raise ValueError(f"Missing addresses for saved nodes: {sorted(unknown)}")
            self.nodes = {name: Node(identity, self.urls[name]) for name, identity in saved.items()}
            self.ring = HashRing(self.nodes)

    # Resolve configured node names to the database identities behind their addresses.
    def discover(self, names):
        if not names or not set(names) <= self.urls.keys() or len(set(names)) != len(names):
            raise ServiceError(400, "Provide one to three distinct configured node names")
        nodes = {name: Node(self.client.identify(self.urls[name]), self.urls[name])
                 for name in names}
        if len({node.identity for node in nodes.values()}) != len(nodes):
            raise ServiceError(409, "Node names must refer to distinct storage volumes")
        return nodes

    # Persist the verified membership before routing requests through the new ring.
    def publish(self, nodes):
        ring = HashRing(nodes)
        self.state.set_setting("membership", {name: node.identity for name, node in nodes.items()})
        self.nodes, self.ring = nodes, ring
        logger.info("Membership active: nodes=%s replication_factor=%d",
                    ",".join(sorted(nodes)), ring.replication_factor)

    # Keep same-key operations serialized, including journal retries.
    @contextmanager
    def key_operation(self, key):
        with self.gate.request(), self.key_locks[hash_value(key) % len(self.key_locks)]:
            if self.restoring:
                raise ServiceError(503, "Restore in progress")
            yield

    # Give the existing API a clear error for keys with an unfinished write.
    def check_pending(self, key):
        if self.state.pending_for(key) is not None:
            raise ServiceError(503, "A write for this key is awaiting replica confirmation")

    # Try the primary first and fall back only when an owner is unavailable.
    def read_record(self, key):
        owners = self.ring.owners(key)
        for name in owners:
            try:
                record = self.client.read(self.nodes[name], key)
                if name != owners[0]:
                    logger.info("Read failover: primary=%s replica=%s", owners[0], name)
                return record
            except NodeUnavailable:
                continue
        raise ServiceError(503, "No owner is available for this key")

    # Hide tombstones from the public API.
    def get(self, key):
        with self.key_operation(key):
            self.check_pending(key)
            record = self.read_record(key)
            return record if record is not None and not record.deleted else None

    # Apply the same journaled record idempotently to each owner.
    def replicate(self, record):
        for name in self.ring.owners(record.key):
            self.client.write(self.nodes[name], record)
        self.state.finish_write(record)

    # Verify owners before opening a new journal entry to preserve reads on outages.
    def mutate(self, key, value, *, deleted=False):
        with self.key_operation(key):
            self.check_pending(key)
            previous = self.read_record(key)
            existed = previous is not None and not previous.deleted
            if deleted and not existed:
                return False
            try:
                for name in self.ring.owners(key):
                    self.client.check(self.nodes[name])
            except NodeUnavailable as exc:
                raise ServiceError(503, "Every owner must be available before writing") from exc
            record = self.state.begin_write(key, value, deleted)
            try:
                self.replicate(record)
            except NodeUnavailable as exc:
                logger.warning("Write pending: key=%r revision=%d reason=%s",
                               key, record.revision, exc)
                raise ServiceError(503, "Write may complete after replica recovery") from exc
            return existed

    # Retry unfinished writes with their original revision after a failure or restart.
    def retry_pending(self):
        for candidate in self.state.pending_records():
            try:
                with self.key_operation(candidate.key):
                    current = self.state.pending_for(candidate.key)
                    if current is not None and current.revision == candidate.revision:
                        self.replicate(current)
                        logger.info("Write recovered: key=%r revision=%d",
                                    current.key, current.revision)
            except ServiceError:
                return
            except NodeUnavailable:
                continue
