"""Consistent hashing over stable node names."""

from bisect import bisect_left
from hashlib import sha256


# Hash keys and virtual nodes consistently across processes and restarts.
def hash_value(value: str) -> int:
    return int.from_bytes(sha256(value.encode("utf-8")).digest(), "big")


class HashRing:
    # Give each physical node several positions to balance the hash space.
    def __init__(self, node_ids, virtual_nodes=128):
        node_ids = sorted(set(node_ids))
        if not node_ids or virtual_nodes < 1:
            raise ValueError("A hash ring needs nodes and at least one position per node")
        self.replication_factor = min(2, len(node_ids))
        self.tokens = sorted(
            (hash_value(f"{node_id}:{index}"), node_id)
            for node_id in node_ids
            for index in range(virtual_nodes)
        )
        self.positions = [position for position, _ in self.tokens]

    # Select the clockwise primary and the next distinct physical node.
    def owners(self, key: str) -> tuple[str, ...]:
        start = bisect_left(self.positions, hash_value(key))
        owners = []
        for offset in range(len(self.tokens)):
            _, node_id = self.tokens[(start + offset) % len(self.tokens)]
            if node_id not in owners:
                owners.append(node_id)
                if len(owners) == self.replication_factor:
                    return tuple(owners)
        raise RuntimeError("Incomplete hash ring")
