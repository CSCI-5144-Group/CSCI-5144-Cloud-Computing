"""Exercise placement, the write journal, and the multi-process HTTP service."""

import asyncio
import csv
import json
import logging
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from urllib.parse import urlsplit

from fastapi import FastAPI
from requests import ConnectionError, Response

from benchmark.__main__ import run_workload
from benchmark.report import write_report
from routing import app as router
from storage import node

from routing.hash_ring import HashRing
from routing.kv_service import KVService, ServiceError
from routing.node_client import Node, NodeClient, NodeUnavailable
from storage.router_state import RouterState

from storage.sqlite_store import NodeStorage


class FaultingClient:
    # Model a node disappearing after preflight and the first replica write.
    def __init__(self):
        self.data = {"a": {}, "b": {}}
        self.failed = None

    def read(self, node, key):
        return self.data[node.url].get(key)

    def check(self, node):
        pass

    def write(self, node, record):
        if node.url == self.failed:
            raise NodeUnavailable("Simulated failure after preflight")
        self.data[node.url][record.key] = record


class StateTests(unittest.TestCase):
    # Adding a node may leave a primary unchanged or transfer it to the new node.
    def test_ring_remaps_only_to_new_node(self):
        old = HashRing(["b", "a"])
        new = HashRing(["a", "b", "c"])
        counts = dict.fromkeys("abc", 0)
        for key in map(str, range(3000)):
            primary, replica = new.owners(key)
            self.assertIn(primary, (old.owners(key)[0], "c"))
            self.assertNotEqual(primary, replica)
            counts[primary] += 1
        self.assertTrue(all(750 < count < 1250 for count in counts.values()), counts)

    # A partial mutation remains private and replays after the router restarts.
    def test_journal_replays_partial_write_after_restart(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "router.sqlite3"
            urls = {name: name for name in "ab"}
            client = FaultingClient()
            service = KVService(RouterState(path), client, urls)
            service.publish({name: Node(name, name) for name in urls})
            first, second = service.ring.owners("alice")
            client.failed = second
            with self.assertRaisesRegex(ServiceError, "may complete"):
                service.mutate("alice", None)
            pending = service.state.pending_for("alice")
            self.assertEqual(client.data[first]["alice"], pending)

            restarted = KVService(RouterState(path), client, urls)
            with self.assertRaisesRegex(ServiceError, "awaiting replica"):
                restarted.get("alice")
            client.failed = None
            restarted.retry_pending()

            self.assertEqual(client.data[second]["alice"], pending)
            self.assertEqual(restarted.get("alice"), pending)
            self.assertIsNone(restarted.state.pending_for("alice"))


# Exercise the real ASGI routes without needing network access or extra libraries.
async def asgi_request(app, method, path, body=None, headers=None):
    messages = asyncio.Queue()
    messages.put_nowait({"type": "http.request", "body": json.dumps(body).encode()})
    output = []

    async def send(message):
        output.append(message)

    all_headers = {"content-type": "application/json", **(headers or {})}
    await app({
        "type": "http", "asgi": {"version": "3.0", "spec_version": "2.4"},
        "http_version": "1.1", "method": method, "scheme": "http",
        "path": path, "root_path": "", "query_string": b"",
        "headers": [(key.lower().encode(), value.encode()) for key, value in all_headers.items()],
    }, messages.get, send)
    response = Response()
    response.status_code = output[0]["status"]
    response._content = b"".join(message.get("body", b"") for message in output[1:])
    return response


class HTTPTests(unittest.IsolatedAsyncioTestCase):
    # Swap only the HTTP transport and give every node its own SQLite database.
    async def asyncSetUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.folder = Path(temporary.name)
        self.nodes = {}
        self.router_context = None
        self.addAsyncCleanup(self.stop_all)
        test = self

        class InProcessSession:
            # Route both node traffic and benchmark traffic through the real handlers.
            def __enter__(self):
                return self

            def __exit__(self, *_):
                pass

            def get(self, url, **kwargs):
                return self.request("GET", url, **kwargs)

            def put(self, url, **kwargs):
                return self.request("PUT", url, **kwargs)

            def request(self, method, url, *, json=None, headers=None, **_):
                parsed = urlsplit(url)
                app = router.app if parsed.hostname == "router" else test.nodes.get(parsed.hostname)
                if app is None:
                    raise ConnectionError("Test node is offline")
                return asyncio.run(asgi_request(app, method, parsed.path, json, headers))

        self.enterContext(patch("routing.node_client.requests.Session", InProcessSession))

    async def start(self, name, *, suffix="", initial="a", configured="abc"):
        path = self.folder / f"{name}{suffix}.sqlite3"
        if name != "router":
            app = FastAPI()
            app.include_router(node.routes)
            app.state.storage = NodeStorage(path)
            self.nodes[name] = app
            return
        env = {
            "ROUTER_DB": str(path), "PROGRESS_INTERVAL": "0.05",
            "NODE_URLS": json.dumps({key: f"http://{key}" for key in configured}),
        }
        with patch.dict(os.environ, env):
            if initial is None:
                os.environ.pop("INITIAL_NODES", None)
            else:
                os.environ["INITIAL_NODES"] = initial
            self.router_context = router.lifespan(router.app)
            await self.router_context.__aenter__()

    async def stop(self, name):
        if name == "router":
            await self.router_context.__aexit__(None, None, None)
            self.router_context = None
        else:
            del self.nodes[name]

    async def stop_all(self):
        if self.router_context is not None:
            await self.stop("router")
        self.nodes.clear()

    async def call(self, method, path, status=200, *, json=None):
        response = await asgi_request(router.app, method, path, json)
        self.assertEqual(response.status_code, status, response.text)
        return response.json()

    # Verify live requests across scale changes, a failed node, and a fresh restore.
    async def test_scale_failover_repair_and_restore(self):
        for name in "abc":
            await self.start(name)
        await self.start("router")
        self.assertEqual((await self.call("GET", "/_admin/status"))["replication_factor"], 1)
        await self.call("PUT", "/null-key", 201, json={"value": None})
        await self.call("PUT", "/_admin/membership", json={"nodes": ["a", "b"]})
        self.assertIsNone((await self.call("GET", "/null-key"))["value"])
        await self.call("PUT", "/_admin/membership", json={"nodes": ["a", "b", "c"]})
        self.assertEqual((await self.call("GET", "/_admin/status"))["replication_factor"], 2)
        await self.call("PUT", "/reject", 400, json={})

        ring = HashRing("abc")
        first = next(key for key in map(str, range(100)) if ring.owners(key)[0] == "a")
        unaffected = next(key for key in map(str, range(100)) if "a" not in ring.owners(key))
        deleted = next(key for key in map(str, range(100, 200)) if ring.owners(key)[0] == "a")
        await self.call("POST", f"/{first}", 201, json={"value": "saved"})
        await self.call("PUT", f"/{unaffected}", 201, json={"value": "original"})
        await self.call("PUT", f"/{deleted}", 201, json={"value": "delete me"})
        await self.call("DELETE", f"/{deleted}")

        await self.stop("a")
        self.assertEqual((await self.call("GET", f"/{first}"))["value"], "saved")
        await self.call("GET", f"/{deleted}", 404)
        await self.call("PUT", f"/{first}", 503, json={"value": "blocked"})
        self.assertEqual((await self.call("GET", f"/{first}"))["value"], "saved")
        await self.call("PUT", f"/{unaffected}", json={"value": "still writable"})

        await self.start("a", suffix="-replacement")
        self.assertEqual((await self.call("GET", f"/{first}"))["value"], "saved")
        await self.call("POST", "/_admin/repair/a")
        await self.stop(ring.owners(first)[1])
        self.assertEqual((await self.call("GET", f"/{first}"))["value"], "saved")
        await self.call("GET", f"/{deleted}", 404)
        await self.start(ring.owners(first)[1])
        await self.stop("router")
        await self.start("router", initial="a")
        self.assertEqual((await self.call("GET", "/_admin/status"))["nodes"], ["a", "b", "c"])

        await self.call("PUT", "/_admin/membership", json={"nodes": ["a", "b"]})
        self.assertEqual((await self.call("GET", f"/{first}"))["value"], "saved")
        snapshot = await self.call("GET", "/_admin/backup")
        self.assertTrue(any(record["deleted"] for record in snapshot["records"]))
        await self.call("POST", "/_admin/restore", 409, json=snapshot)
        await self.stop_all()

        await self.start("a", suffix="-restored")
        await self.start("router", suffix="-restored")
        await self.call("POST", "/_admin/restore", json=snapshot)
        self.assertEqual((await self.call("GET", f"/{first}"))["value"], "saved")
        self.assertEqual((await self.call("GET", f"/{unaffected}"))["value"], "still writable")
        self.assertIsNone((await self.call("GET", "/null-key"))["value"])
        await self.call("GET", f"/{deleted}", 404)
        await self.call("PUT", f"/{first}", json={"value": "new revision"})
        self.assertGreater((await self.call("GET", "/_admin/backup"))["clock"], snapshot["clock"])
        with self.assertLogs(logging.getLogger("uvicorn.error"), level="INFO") as logs:
            await asyncio.sleep(0.15)
        self.assertTrue(any("Progress: active_nodes" in line for line in logs.output))


    # A partial restore stays hidden across router restart and can replay its batches.
    async def test_interrupted_restore_resumes_after_restart(self):
        for name in "ab":
            await self.start(name)
        await self.start("router", initial="a,b")
        snapshot = {
            "format_version": 1, "clock": 7,
            "records": [{"key": "alice", "revision": 7, "deleted": False, "value": True}],
        }
        merge = NodeClient.merge_records

        def fail_second_node(client, target, records):
            if target.url == "http://b":
                raise NodeUnavailable("Simulated failure during restore")
            merge(client, target, records)

        with patch.object(NodeClient, "merge_records", fail_second_node):
            await self.call("POST", "/_admin/restore", 503, json=snapshot)
        await self.stop("router")
        await self.start("router")
        self.assertTrue((await self.call("GET", "/_admin/status"))["restoring"])
        await self.call("GET", "/alice", 503)
        altered = {**snapshot, "records": [{**snapshot["records"][0], "value": 1}]}
        await self.call("POST", "/_admin/restore", 409, json=altered)
        await self.call("POST", "/_admin/restore", json=snapshot)
        self.assertIs((await self.call("GET", "/alice"))["value"], True)
        self.assertFalse((await self.call("GET", "/_admin/status"))["restoring"])


    # Prove that membership uses configured names and survives router restart.
    async def test_custom_node_names(self):
        for name in "def":
            await self.start(name)
        await self.start("router", initial=None, configured="def")
        self.assertEqual((await self.call("GET", "/_admin/status"))["nodes"], list("def"))
        await self.call("PUT", "/alice", 201, json={"value": "saved"})
        await self.call("PUT", "/_admin/membership", json={"nodes": ["d"]})
        await self.call("PUT", "/_admin/membership", json={"nodes": ["d", "e", "f"]})
        await self.call("PUT", "/_admin/membership", 400, json={"nodes": ["d", "a"]})
        await self.stop("router")
        await self.start("router", initial="d", configured="def")
        self.assertEqual((await self.call("GET", "/_admin/status"))["nodes"], list("def"))
        self.assertEqual((await self.call("GET", "/alice"))["value"], "saved")

    # Verify the measured workload and output files without persisting test timings.
    async def test_benchmark_workload_and_reports(self):
        args = SimpleNamespace(keys=8, workers=2, trials=1, value_bytes=16)
        rows, failures = [], []
        for count in (1, 2, 3):
            names = "abc"[:count]
            suffix = f"-benchmark-{count}"
            for name in names:
                await self.start(name, suffix=suffix)
            await self.start("router", initial=",".join(names), suffix=suffix)
            await asyncio.to_thread(run_workload, "http://router", count, args, rows, failures)
            await self.stop_all()
        self.assertFalse(failures)
        self.assertEqual(len(rows), 6)
        self.assertTrue(all(row.successful == 8 and row.p99_ms > 0 for row in rows))
        output = self.folder / "reports"
        write_report(output, rows, dict(vars(args), host="test only"), failures)
        with (output / "trials.csv").open() as file:
            self.assertEqual(len(list(csv.DictReader(file))), 6)
        self.assertIn("| 3 | 2 | GET | 1 / 1 |", (output / "report.md").read_text())
        self.assertEqual((output / "charts.html").read_text().count("<section>"), 4)


if __name__ == "__main__":
    unittest.main()
