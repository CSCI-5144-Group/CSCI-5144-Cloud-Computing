"""
Single-server key-value store.

Endpoints (match the provided benchmark):
    GET    /<key>                      -> retrieve value
    POST   /<key>  body {"value": ...} -> store value   (PUT /<key> also accepted)
    DELETE /<key>                      -> remove key

Features:
    * Concurrency: Flask's threaded server handles requests in parallel; a lock
      makes every read/modify on the store atomic, so concurrent PUT/DEL on the
      same key can never interleave or corrupt state.
    * Persistence: every PUT/DEL is appended to a write-ahead log (WAL) before
      it is applied. A background thread periodically writes a full snapshot
      and truncates the WAL. On startup: load snapshot, then replay the WAL.
    * Error handling: JSON error messages with proper HTTP status codes.
    * Logging: every GET/PUT/DEL is logged with a timestamp to kvstore.log.
"""

import argparse
import json
import logging
import os
import signal
import sys
import threading
import time

from flask import Flask, jsonify, request

DATA_DIR = os.environ.get("KV_DATA_DIR", "data")
SNAPSHOT_FILE = os.path.join(DATA_DIR, "snapshot.json")
WAL_FILE = os.path.join(DATA_DIR, "wal.log")
OP_LOG_FILE = "kvstore.log"
SNAPSHOT_INTERVAL = 10  # seconds

# ---------------------------------------------------------------- logging ---
logger = logging.getLogger("kvstore")
logger.setLevel(logging.INFO)
_handler = logging.FileHandler(OP_LOG_FILE)
_handler.setFormatter(logging.Formatter("%(asctime)s.%(msecs)03d | %(message)s",
                                        datefmt="%Y-%m-%d %H:%M:%S"))
logger.addHandler(_handler)
logging.getLogger("werkzeug").setLevel(logging.WARNING)  # quiet per-request HTTP noise


# ------------------------------------------------------------------ store ---
class KVStore:
    def __init__(self):
        self._data = {}
        self._lock = threading.Lock()
        self._dirty = False
        os.makedirs(DATA_DIR, exist_ok=True)
        self._recover()
        self._wal = open(WAL_FILE, "a", encoding="utf-8")

    # -- recovery --------------------------------------------------------
    def _recover(self):
        if os.path.exists(SNAPSHOT_FILE):
            with open(SNAPSHOT_FILE, encoding="utf-8") as f:
                self._data = json.load(f)
        replayed = 0
        if os.path.exists(WAL_FILE):
            with open(WAL_FILE, encoding="utf-8") as f:
                for line in f:
                    try:
                        entry = json.loads(line)
                    except json.JSONDecodeError:
                        break  # partial last line from a crash: ignore it
                    if entry["op"] == "PUT":
                        self._data[entry["key"]] = entry["value"]
                    elif entry["op"] == "DEL":
                        self._data.pop(entry["key"], None)
                    replayed += 1
        logger.info(f"RECOVER | {len(self._data)} keys loaded, {replayed} WAL entries replayed")

    def _append_wal(self, entry):
        self._wal.write(json.dumps(entry) + "\n")
        self._wal.flush()

    # -- operations ------------------------------------------------------
    def get(self, key):
        with self._lock:
            return self._data.get(key), key in self._data

    def put(self, key, value):
        with self._lock:
            self._append_wal({"op": "PUT", "key": key, "value": value})
            existed = key in self._data
            self._data[key] = value
            self._dirty = True
            return existed

    def delete(self, key):
        with self._lock:
            if key not in self._data:
                return False
            self._append_wal({"op": "DEL", "key": key})
            del self._data[key]
            self._dirty = True
            return True

    # -- persistence -----------------------------------------------------
    def snapshot(self):
        """Write the full store to disk atomically, then truncate the WAL."""
        with self._lock:
            if not self._dirty:
                return
            tmp = SNAPSHOT_FILE + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self._data, f)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, SNAPSHOT_FILE)  # atomic: never a half-written snapshot
            self._wal.close()
            self._wal = open(WAL_FILE, "w", encoding="utf-8")  # truncate
            self._dirty = False
            n = len(self._data)
        logger.info(f"SNAPSHOT | {n} keys saved")

    def snapshot_loop(self):
        while True:
            time.sleep(SNAPSHOT_INTERVAL)
            try:
                self.snapshot()
            except Exception as e:
                logger.info(f"SNAPSHOT_ERROR | {e}")


# -------------------------------------------------------------------- app ---
app = Flask(__name__)
store = KVStore()


def error(status, message):
    return jsonify({"error": message}), status


@app.route("/<key>", methods=["GET"])
def get_key(key):
    value, found = store.get(key)
    if not found:
        logger.info(f"GET | key={key} | NOT_FOUND")
        return error(404, f"Key '{key}' not found")
    logger.info(f"GET | key={key} | OK")
    return jsonify({"key": key, "value": value}), 200


@app.route("/<key>", methods=["POST", "PUT"])
def put_key(key):
    body = request.get_json(silent=True)
    if not isinstance(body, dict) or "value" not in body:
        logger.info(f"PUT | key={key} | BAD_REQUEST")
        return error(400, 'Request body must be JSON of the form {"value": ...}')
    existed = store.put(key, body["value"])
    logger.info(f"PUT | key={key} | {'UPDATED' if existed else 'CREATED'}")
    return jsonify({"key": key, "value": body["value"]}), (200 if existed else 201)


@app.route("/<key>", methods=["DELETE"])
def delete_key(key):
    if not store.delete(key):
        logger.info(f"DEL | key={key} | NOT_FOUND")
        return error(404, f"Key '{key}' not found")
    logger.info(f"DEL | key={key} | OK")
    return jsonify({"key": key, "deleted": True}), 200


@app.errorhandler(405)
def method_not_allowed(_):
    return error(405, "Method not allowed. Use GET, POST/PUT or DELETE on /<key>")


@app.errorhandler(404)
def not_found(_):
    return error(404, "Not found. Use /<key>")


@app.errorhandler(Exception)
def internal_error(e):
    logger.info(f"ERROR | {type(e).__name__}: {e}")
    return error(500, "Internal server error")


def shutdown(*_):
    store.snapshot()
    logger.info("SHUTDOWN")
    sys.exit(0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Single-server key-value store")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)
    threading.Thread(target=store.snapshot_loop, daemon=True).start()

    logger.info(f"START | listening on {args.host}:{args.port}")
    print(f"KV store listening on http://{args.host}:{args.port}")
    app.run(host=args.host, port=args.port, threaded=True)
