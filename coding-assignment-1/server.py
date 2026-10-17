"""
Single-server key-value store, FastAPI version (for comparison with server.py).

Same API, same store design (lock striping, write-ahead log, snapshots,
timestamped logging) as server.py; only the web framework changes.

Run:
    python3 server_fastapi.py                # sync routes (default)
    python3 server_fastapi.py --mode async   # async routes

The two modes show the two ways FastAPI can run your code:
    * sync  ("def" routes): FastAPI runs each request on a thread from a pool,
      like Flask. The locks protect the store exactly as in server.py.
    * async ("async def" routes): every request runs on ONE event-loop thread.
      No two requests touch the store at the same instant, but any blocking
      call (like the WAL file write) pauses all requests until it finishes.
"""

import argparse
import json
import logging
import logging.handlers
import os
import queue
import threading
import time
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

DATA_DIR = os.environ.get("KV_DATA_DIR", "data")
SNAPSHOT_FILE = os.path.join(DATA_DIR, "snapshot.json")
WAL_FILE = os.path.join(DATA_DIR, "wal.log")
OLD_WAL_FILE = os.path.join(DATA_DIR, "wal.old")
OP_LOG_FILE = "kvstore.log"
SNAPSHOT_INTERVAL = 10  # seconds
NUM_STRIPES = 16        # number of lock groups

# ---------------------------------------------------------------- logging ---
_log_queue = queue.Queue(-1)
_file_handler = logging.FileHandler(OP_LOG_FILE)
_file_handler.setFormatter(logging.Formatter("%(asctime)s.%(msecs)03d | %(message)s",
                                             datefmt="%Y-%m-%d %H:%M:%S"))
_log_listener = logging.handlers.QueueListener(_log_queue, _file_handler)
_log_listener.start()

logger = logging.getLogger("kvstore")
logger.setLevel(logging.INFO)
logger.propagate = False
logger.addHandler(logging.handlers.QueueHandler(_log_queue))


# ------------------------------------------------------------------ store ---
# Identical to the KVStore class in server.py.
class KVStore:
    def __init__(self):
        self._data = {}
        self._locks = [threading.Lock() for _ in range(NUM_STRIPES)]
        self._wal_lock = threading.Lock()
        self._dirty = False
        os.makedirs(DATA_DIR, exist_ok=True)
        self._recover()
        self._wal = open(WAL_FILE, "a", encoding="utf-8")

    def _lock_for(self, key):
        return self._locks[hash(key) % NUM_STRIPES]

    def _replay(self, path):
        count = 0
        if not os.path.exists(path):
            return count
        with open(path, encoding="utf-8") as f:
            for line in f:
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    break
                if entry["op"] == "PUT":
                    self._data[entry["key"]] = entry["value"]
                elif entry["op"] == "DEL":
                    self._data.pop(entry["key"], None)
                count += 1
        return count

    def _recover(self):
        if os.path.exists(SNAPSHOT_FILE):
            with open(SNAPSHOT_FILE, encoding="utf-8") as f:
                self._data = json.load(f)
        replayed = self._replay(OLD_WAL_FILE) + self._replay(WAL_FILE)
        logger.info(f"RECOVER | {len(self._data)} keys loaded, {replayed} WAL entries replayed")

    def _append_wal(self, entry):
        with self._wal_lock:
            self._wal.write(json.dumps(entry) + "\n")
            self._wal.flush()

    def get(self, key):
        with self._lock_for(key):
            return self._data.get(key), key in self._data

    def put(self, key, value):
        with self._lock_for(key):
            self._append_wal({"op": "PUT", "key": key, "value": value})
            existed = key in self._data
            self._data[key] = value
            self._dirty = True
            return existed

    def delete(self, key):
        with self._lock_for(key):
            if key not in self._data:
                return False
            self._append_wal({"op": "DEL", "key": key})
            del self._data[key]
            self._dirty = True
            return True

    def snapshot(self):
        for lock in self._locks:
            lock.acquire()
        try:
            if not self._dirty:
                return
            data_copy = dict(self._data)
            with self._wal_lock:
                self._wal.close()
                os.replace(WAL_FILE, OLD_WAL_FILE)
                self._wal = open(WAL_FILE, "a", encoding="utf-8")
            self._dirty = False
        finally:
            for lock in self._locks:
                lock.release()
        tmp = SNAPSHOT_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data_copy, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, SNAPSHOT_FILE)
        os.remove(OLD_WAL_FILE)
        logger.info(f"SNAPSHOT | {len(data_copy)} keys saved")

    def snapshot_loop(self):
        while True:
            time.sleep(SNAPSHOT_INTERVAL)
            try:
                self.snapshot()
            except Exception as e:
                logger.info(f"SNAPSHOT_ERROR | {e}")


store = KVStore()


# ---------------------------------------------------------- request logic ---
# Shared by both modes; returns (status_code, body).
def do_get(key):
    value, found = store.get(key)
    if not found:
        logger.info(f"GET | key={key} | NOT_FOUND")
        return 404, {"error": f"Key '{key}' not found"}
    logger.info(f"GET | key={key} | OK")
    return 200, {"key": key, "value": value}


def do_put(key, raw_body):
    try:
        body = json.loads(raw_body)
    except (json.JSONDecodeError, UnicodeDecodeError):
        body = None
    if not isinstance(body, dict) or "value" not in body:
        logger.info(f"PUT | key={key} | BAD_REQUEST")
        return 400, {"error": 'Request body must be JSON of the form {"value": ...}'}
    existed = store.put(key, body["value"])
    logger.info(f"PUT | key={key} | {'UPDATED' if existed else 'CREATED'}")
    return (200 if existed else 201), {"key": key, "value": body["value"]}


def do_delete(key):
    if not store.delete(key):
        logger.info(f"DEL | key={key} | NOT_FOUND")
        return 404, {"error": f"Key '{key}' not found"}
    logger.info(f"DEL | key={key} | OK")
    return 200, {"key": key, "deleted": True}


def respond(result):
    status, body = result
    return JSONResponse(body, status_code=status)


# ------------------------------------------------------------------- apps ---
@asynccontextmanager
async def lifespan(app):
    yield                      # server runs while we're paused here
    store.snapshot()           # runs once on shutdown (Ctrl+C)
    logger.info("SHUTDOWN")
    _log_listener.stop()


def make_app(mode):
    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None, lifespan=lifespan)

    if mode == "sync":
        # "def" routes: FastAPI runs each one on a worker thread.
        # (The body is read asynchronously first, then handed to the thread.)
        from starlette.concurrency import run_in_threadpool

        @app.get("/{key}")
        def get_key(key: str):
            return respond(do_get(key))

        @app.api_route("/{key}", methods=["POST", "PUT"])
        async def put_key(key: str, request: Request):
            raw = await request.body()
            return respond(await run_in_threadpool(do_put, key, raw))

        @app.delete("/{key}")
        def delete_key(key: str):
            return respond(do_delete(key))
    else:
        # "async def" routes: everything runs on the single event-loop thread.
        @app.get("/{key}")
        async def get_key(key: str):
            return respond(do_get(key))

        @app.api_route("/{key}", methods=["POST", "PUT"])
        async def put_key(key: str, request: Request):
            return respond(do_put(key, await request.body()))

        @app.delete("/{key}")
        async def delete_key(key: str):
            return respond(do_delete(key))

    @app.exception_handler(405)
    async def method_not_allowed(request, exc):
        return JSONResponse({"error": "Method not allowed. Use GET, POST/PUT or DELETE on /<key>"},
                            status_code=405)

    @app.exception_handler(404)
    async def not_found(request, exc):
        return JSONResponse({"error": "Not found. Use /<key>"}, status_code=404)

    @app.exception_handler(Exception)
    async def internal_error(request, exc):
        logger.info(f"ERROR | {type(exc).__name__}: {exc}")
        return JSONResponse({"error": "Internal server error"}, status_code=500)

    return app


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Single-server key-value store (FastAPI)")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--mode", choices=["sync", "async"], default="sync")
    args = parser.parse_args()

    threading.Thread(target=store.snapshot_loop, daemon=True).start()
    logger.info(f"START | fastapi-{args.mode} server on {args.host}:{args.port}")
    print(f"KV store (FastAPI, {args.mode}) listening on http://{args.host}:{args.port}", flush=True)
    # uvicorn handles Ctrl+C itself and then runs the shutdown hook above.
    uvicorn.run(make_app(args.mode), host=args.host, port=args.port,
                log_level="warning", access_log=False)
