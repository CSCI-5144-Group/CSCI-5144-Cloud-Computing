"""Manage placement changes and replacement nodes."""

import logging

from routing.hash_ring import HashRing
from routing.kv_service import ServiceError
from storage.records import same_record

logger = logging.getLogger("uvicorn.error")
BATCH_SIZE = 500


class Maintenance:
    # Share the live router state and transport with the maintenance operations.
    def __init__(self, service):
        self.service = service
        self.client = service.client
        self.state = service.state

    # Finish pending mutations before changing their assigned owners.
    def require_settled(self):
        if self.state.pending_count():
            raise ServiceError(503, "Finish pending writes before this operation")

    # Collect the newest record from its old owners, skipping stale non-owner data.
    def collect(self, *, skip=None):
        latest = {}
        for name, node in self.service.nodes.items():
            if name == skip:
                continue
            rows = self.client.records(node)
            logger.info("Maintenance scan: node=%s records=%d", name, len(rows))
            for record in rows:
                if name not in self.service.ring.owners(record.key):
                    continue
                previous = latest.get(record.key)
                if previous is None or record.revision > previous.revision:
                    latest[record.key] = record
                elif record.revision == previous.revision and not same_record(record, previous):
                    raise ServiceError(409, f"Conflicting copies of {record.key!r}")
        return latest

    # Accept an empty target or compatible data left by an earlier placement.
    def check_target(self, node, authoritative):
        for record in self.client.records(node):
            expected = authoritative.get(record.key)
            if expected is None or record.revision > expected.revision or (
                record.revision == expected.revision and not same_record(record, expected)
            ):
                raise ServiceError(409, "Target contains unexpected data; restart the target node to clear its data")

    # Copy records in batches and confirm the target holds the intended revisions.
    def install(self, name, node, records):
        total = len(records)
        logger.info("Installing records: node=%s total=%d", name, total)
        for start in range(0, total, BATCH_SIZE):
            batch = records[start:start + BATCH_SIZE]
            self.client.merge_records(node, batch)
            logger.info("Install progress: node=%s records=%d/%d",
                        name, start + len(batch), total)
        actual = {record.key: record for record in self.client.records(node)}
        if any(not same_record(actual.get(record.key), record) for record in records):
            raise ServiceError(503, f"Record verification failed on node {name}")
        logger.info("Verified placement: node=%s records=%d", name, total)

    # Group each logical record only under its current primary and replica.
    def place(self, nodes, records):
        ring = HashRing(nodes)
        placements = {name: [] for name in nodes}
        for record in records:
            for name in ring.owners(record.key):
                placements[name].append(record)
        for name, node in nodes.items():
            self.install(name, node, placements[name])

    # Populate the new ring before publishing it. Copies in old positions are retained.
    def resize(self, names):
        with self.service.gate.exclusive():
            self.require_settled()
            logger.info("Membership change started: target=%s", ",".join(names))
            nodes = self.service.discover(names)
            for name in self.service.nodes.keys() & nodes.keys():
                if self.service.nodes[name] != nodes[name]:
                    raise ServiceError(409, f"Repair the replacement for {name} before resizing")
            latest = self.collect()
            for name in nodes.keys() - self.service.nodes.keys():
                self.check_target(nodes[name], latest)
            self.place(nodes, latest.values())
            self.service.publish(nodes)
        return {"nodes": sorted(nodes), "records": len(latest)}

    # Rebuild a replacement from surviving owners without trusting its old identity.
    def repair(self, name):
        with self.service.gate.exclusive():
            if name not in self.service.nodes:
                raise ServiceError(404, "Node is not in the active membership")
            if self.service.ring.replication_factor < 2:
                raise ServiceError(409, "A single-node store has no surviving replica for replacement")
            logger.info("Replica repair started: node=%s", name)
            replacement = self.service.discover([name])[name]
            if replacement == self.service.nodes[name]:
                raise ServiceError(409, "The original process is already enrolled")
            nodes = dict(self.service.nodes, **{name: replacement})
            if len({node.identity for node in nodes.values()}) != len(nodes):
                raise ServiceError(409, "The replacement must be a distinct process")
            latest = self.collect(skip=name)
            needed = {key: record for key, record in latest.items()
                      if name in self.service.ring.owners(key)}
            self.check_target(replacement, needed)
            self.install(name, replacement, list(needed.values()))
            self.service.publish(nodes)
            logger.info("Replica repair finished: node=%s records=%d", name, len(needed))
        return {"node": name, "records": len(needed)}
