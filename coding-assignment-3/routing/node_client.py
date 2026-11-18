"""HTTP transport between the router and its storage nodes."""

from dataclasses import dataclass
from threading import local

import requests

from storage.records import Record


@dataclass(frozen=True)
class Node:
    identity: str
    url: str


class NodeUnavailable(Exception):
    """The router could not get a usable response from a storage node."""


class NodeConflict(NodeUnavailable):
    """The node rejected its identity or record revision."""


class NodeClient:
    # Reuse connections within each router worker thread.
    def __init__(self, timeout=1.0, maintenance_timeout=30.0):
        self.local = local()
        self.timeout = timeout
        self.maintenance_timeout = maintenance_timeout

    # Convert transport and server errors into failures the router can handle.
    def request(self, method, url, *, identity=None, body=None, maintenance=False):
        if not hasattr(self.local, "session"):
            self.local.session = requests.Session()
            self.local.session.trust_env = False
        headers = {"X-Node-Identity": identity} if identity else {}
        try:
            response = self.local.session.request(
                method, url, json=body, headers=headers, allow_redirects=False,
                timeout=self.maintenance_timeout if maintenance else self.timeout,
            )
            if response.status_code == 409:
                raise NodeConflict(f"{method} {url}: storage node rejected the operation")
            if not response.ok or response.is_redirect:
                raise NodeUnavailable(f"{method} {url}: HTTP {response.status_code}")
            return None if response.status_code == 204 else response.json()
        except (requests.RequestException, ValueError) as exc:
            raise NodeUnavailable(f"{method} {url}: {exc}") from exc

    # Discover the identity of the process currently behind an address.
    def identify(self, url):
        return self.request("GET", f"{url}/health")["identity"]

    # Check that a write target is reachable and still uses its enrolled process.
    def check(self, node):
        if self.identify(node.url) != node.identity:
            raise NodeConflict(f"{node.url}: process must be enrolled before use")

    # Fetch a record from an enrolled node.
    def read(self, node, key):
        data = self.request("POST", f"{node.url}/internal/read",
                            identity=node.identity, body={"key": key})
        return Record.model_validate(data) if data is not None else None

    # Send a versioned mutation to an enrolled node.
    def write(self, node, record):
        self.request("PUT", f"{node.url}/internal/record", identity=node.identity,
                     body=record.model_dump(mode="json"))

    # Read a complete node snapshot for maintenance.
    def records(self, node):
        data = self.request("GET", f"{node.url}/internal/records", identity=node.identity,
                            maintenance=True)
        return [Record.model_validate(item) for item in data]

    # Install records while preserving the node's revision checks.
    def merge_records(self, node, records):
        self.request("PUT", f"{node.url}/internal/records", identity=node.identity,
                     body=[record.model_dump(mode="json") for record in records], maintenance=True)
