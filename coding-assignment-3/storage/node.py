"""Internal HTTP service for one in-memory KV node."""

import logging
import os
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import APIRouter, Depends, FastAPI, Header, HTTPException, Request
from pydantic import BaseModel

from storage.records import Record
from storage.memory_store import ConflictingRevision, NodeStorage

logger = logging.getLogger("uvicorn.error")


class KeyRequest(BaseModel):
    key: str


# Create this process's independent store before serving requests.
@asynccontextmanager
async def lifespan(app):
    app.state.storage = NodeStorage()
    logger.info("Storage node ready: name=%s identity=%s", os.environ.get("NODE_NAME", "local"),
                app.state.storage.identity)
    yield


routes = APIRouter()


# Reject normal requests routed to an unenrolled node process.
def enrolled_storage(request: Request, x_node_identity: Annotated[str, Header()]) -> NodeStorage:
    storage = request.app.state.storage
    if storage.identity != x_node_identity:
        raise HTTPException(409, "Node identity does not match")
    return storage


EnrolledStorage = Annotated[NodeStorage, Depends(enrolled_storage)]


# Announce the local identity for bootstrap and explicit enrollment.
@routes.get("/health")
def health(request: Request):
    return {"identity": request.app.state.storage.identity}


# Fetch the local record so the router can distinguish absence from failure.
@routes.post("/internal/read")
def read(body: KeyRequest, storage: EnrolledStorage):
    return storage.read(body.key)


# Apply a versioned write or tombstone to this node only.
@routes.put("/internal/record", status_code=204)
def write(record: Record, storage: EnrolledStorage):
    try:
        storage.merge([record])
    except ConflictingRevision as exc:
        raise HTTPException(409, "Conflicting record revision") from exc


# Export records for maintenance while preserving tombstones.
@routes.get("/internal/records")
def records(storage: EnrolledStorage):
    return storage.records()


# Merge a repair batch without allowing delayed requests to roll back newer data.
@routes.put("/internal/records", status_code=204)
def merge_records(records: list[Record], storage: EnrolledStorage):
    try:
        storage.merge(records)
    except ConflictingRevision as exc:
        raise HTTPException(409, "Conflicting record revision in batch") from exc


app = FastAPI(title="KV storage node", lifespan=lifespan)
app.include_router(routes)
