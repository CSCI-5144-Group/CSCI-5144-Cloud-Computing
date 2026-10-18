"""
Version 1: a single-server key-value store.

Run: python server.py
Data is lost when the process exits. Use exactly one server process.
"""

import argparse
import json
import logging
import threading
from typing import Any

import uvicorn
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, field_validator

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
logger = logging.getLogger("kvstore")
app = FastAPI(title="Key-Value Store", version="1.0")
store = {}
lock = threading.Lock()


class PutRequest(BaseModel):
    value: Any  # Required, but JSON null is a valid value.

    @field_validator("value")
    @classmethod
    def validate_json(cls, value):
        json.dumps(value, allow_nan=False)
        return value


@app.get("/{key}")
def get_key(key: str):
    with lock:
        if key not in store:
            logger.info("GET | key=%r | NOT_FOUND", key)
            return JSONResponse({"error": f"Key '{key}' not found"}, status_code=404)
        value = store[key]
        logger.info("GET | key=%r | OK", key)
    return {"key": key, "value": value}


@app.put("/{key}")
@app.post("/{key}", include_in_schema=False)
def put_key(key: str, body: PutRequest):
    with lock:
        existed = key in store
        store[key] = body.value
        logger.info("PUT | key=%r | %s", key, "UPDATED" if existed else "CREATED")
    return JSONResponse(
        {"key": key, "value": body.value}, status_code=200 if existed else 201
    )


@app.delete("/{key}")
def delete_key(key: str):
    with lock:
        if key not in store:
            logger.info("DEL | key=%r | NOT_FOUND", key)
            return JSONResponse({"error": f"Key '{key}' not found"}, status_code=404)
        del store[key]
        logger.info("DEL | key=%r | OK", key)
    return {"key": key, "deleted": True}


@app.exception_handler(RequestValidationError)
async def invalid_request(request: Request, exc: RequestValidationError):
    logger.info("%s | path=%r | BAD_REQUEST", request.method, request.url.path)
    return JSONResponse(
        {
            "error": 'Invalid request. Write body must be JSON of the form {"value": ...}'
        },
        status_code=400,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()
    uvicorn.run(app, host=args.host, port=args.port, access_log=False)
