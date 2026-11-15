"""Single public HTTP entry point for the distributed KV service."""

import asyncio
import json
import logging
import os
import sqlite3
from collections import Counter
from contextlib import asynccontextmanager
from pathlib import Path
from threading import Event, Lock, Thread
from time import monotonic, sleep

from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from management.api import routes as admin_routes
from management.maintenance import Maintenance
from routing.api import routes as public_routes
from routing.kv_service import KVService, ServiceError
from routing.node_client import NodeClient, NodeConflict, NodeUnavailable
from storage.router_state import RouterState

logger = logging.getLogger("uvicorn.error")
LOCAL_URLS = {"a": "http://127.0.0.1:8081", "b": "http://127.0.0.1:8082",
              "c": "http://127.0.0.1:8083"}


class Progress:
    # Keep request status counters independent from routing and persistence.
    def __init__(self):
        self.counts = Counter()
        self.lock = Lock()

    # Record the response after the HTTP middleware receives its status.
    def record(self, status):
        with self.lock:
            self.counts[status] += 1

    # Snapshot the counters for the progress thread.
    def summary(self):
        with self.lock:
            return dict(sorted(self.counts.items()))


# Recover existing routing state or enroll empty nodes on the first boot.
def start_service():
    urls = json.loads(os.environ.get("NODE_URLS", json.dumps(LOCAL_URLS)))
    if not isinstance(urls, dict) or not 1 <= len(urls) <= 3 or any(
        not isinstance(url, str) or not url.startswith(("http://", "https://"))
        for url in urls.values()
    ):
        raise ValueError("NODE_URLS must map up to three node names to HTTP URLs")
    service = KVService(
        RouterState(Path(os.environ.get("ROUTER_DB", "data/router.sqlite3"))),
        NodeClient(), {name: url.rstrip("/") for name, url in urls.items()},
    )
    if service.ring is None:
        initial = os.environ.get("INITIAL_NODES", ",".join(urls))
        names = [name.strip() for name in initial.split(",")]
        deadline = monotonic() + 30
        while True:
            try:
                nodes = service.discover(names)
                if any(service.client.records(node) for node in nodes.values()):
                    raise ValueError("New router requires empty nodes; restore from a backup")
                service.publish(nodes)
                break
            except NodeUnavailable as exc:
                if monotonic() >= deadline:
                    raise
                logger.info("Waiting for initial nodes: %s", exc)
                sleep(1)
    logger.info("Router ready: nodes=%s restoring=%s",
                ",".join(sorted(service.nodes)), service.restoring)
    return service


# Retry pending writes in a separate loop so progress reporting stays responsive.
def recovery_loop(service, stopped):
    while not stopped.wait(2):
        try:
            service.retry_pending()
        except Exception:
            logger.exception("Pending-write recovery pass failed")


# Keep reporting status even when the router has no incoming requests.
def progress_loop(service, progress, stopped, interval):
    while not stopped.wait(interval):
        try:
            logger.info("Progress: active_nodes=%s responses=%s pending_writes=%d restoring=%s",
                        ",".join(sorted(service.nodes)), progress.summary(),
                        service.state.pending_count(), service.restoring)
        except sqlite3.Error:
            logger.exception("Could not report journal progress")


# Run a single Uvicorn worker so same-key and maintenance locks share one owner.
@asynccontextmanager
async def lifespan(app):
    interval = float(os.environ.get("PROGRESS_INTERVAL", "5"))
    if interval <= 0:
        raise ValueError("PROGRESS_INTERVAL must be positive")
    app.state.service = service = await asyncio.to_thread(start_service)
    app.state.maintenance = Maintenance(service)
    app.state.progress = progress = Progress()
    stopped = Event()
    threads = [
        Thread(target=recovery_loop, args=(service, stopped), daemon=True),
        Thread(target=progress_loop, args=(service, progress, stopped, interval), daemon=True),
    ]
    for thread in threads:
        thread.start()
    try:
        yield
    finally:
        stopped.set()
        for thread in threads:
            thread.join(timeout=2)


app = FastAPI(title="Distributed KV router", lifespan=lifespan)
app.include_router(admin_routes)
app.include_router(public_routes)


# Count public and management responses without logging every individual request.
@app.middleware("http")
async def count_responses(request, call_next):
    response = await call_next(request)
    request.app.state.progress.record(response.status_code)
    if request.url.path.startswith("/_admin/"):
        logger.info("Management response: %s %s status=%d",
                    request.method, request.url.path, response.status_code)
    return response


# Translate routing and storage failures into public HTTP responses.
@app.exception_handler(ServiceError)
def service_error(request, exc):
    return JSONResponse({"error": str(exc)}, status_code=exc.status)


# Distinguish an enrollment conflict from a storage outage.
@app.exception_handler(NodeUnavailable)
def node_error(request, exc):
    logger.warning("Storage operation failed: %s", exc)
    status = 409 if isinstance(exc, NodeConflict) else 503
    return JSONResponse({"error": str(exc)}, status_code=status)


# Preserve the Assignment 2 bad-request response for invalid public writes.
@app.exception_handler(RequestValidationError)
def validation_error(request, exc):
    message = "Invalid request"
    if request.method in {"PUT", "POST"} and len(request.url.path.strip("/").split("/")) == 1:
        message += '. Write body must be JSON of the form {"value": ...}'
    return JSONResponse({"error": message}, status_code=400)
