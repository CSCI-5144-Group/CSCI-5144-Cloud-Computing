"""Public KV HTTP endpoints."""

from typing import Annotated

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse

from routing.kv_service import KVService, ServiceError
from storage.records import WriteBody

routes = APIRouter()


# Resolve the single service instance for public and management routes.
def get_service(request: Request):
    return request.app.state.service


Service = Annotated[KVService, Depends(get_service)]


# Keep the public method, response shape, and missing-key semantics from Assignment 2.
@routes.get("/{key}")
def get_key(key: str, service: Service):
    record = service.get(key)
    if record is None:
        raise ServiceError(404, f"Key '{key}' not found")
    return {"key": key, "value": record.value}


# Preserve the PUT response and benchmark-compatible POST alias.
@routes.put("/{key}")
@routes.post("/{key}", include_in_schema=False)
def put_key(key: str, body: WriteBody, service: Service):
    existed = service.mutate(key, body.value)
    return JSONResponse({"key": key, "value": body.value}, status_code=200 if existed else 201)


# Return success only for an existing key with a confirmed tombstone.
@routes.delete("/{key}")
def delete_key(key: str, service: Service):
    if not service.mutate(key, None, deleted=True):
        raise ServiceError(404, f"Key '{key}' not found")
    return {"key": key, "deleted": True}
