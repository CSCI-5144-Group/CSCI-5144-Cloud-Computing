"""Administrative HTTP endpoints for cluster maintenance."""

from typing import Annotated

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel

from management.maintenance import Maintenance, Snapshot

routes = APIRouter(prefix="/_admin")


class MembershipRequest(BaseModel):
    nodes: list[str]


# Resolve the maintenance service initialized by the router.
def get_maintenance(request: Request):
    return request.app.state.maintenance


Admin = Annotated[Maintenance, Depends(get_maintenance)]


# Expose status on a two-segment path so user keys retain their original namespace.
@routes.get("/status")
def status(maintenance: Admin):
    service = maintenance.service
    return {
        "nodes": sorted(service.nodes),
        "replication_factor": service.ring.replication_factor,
        "pending_writes": service.state.pending_count(),
        "restoring": service.restoring,
    }


# Delegate planned resizing to the maintenance service.
@routes.put("/membership")
def update_membership(body: MembershipRequest, maintenance: Admin):
    return maintenance.resize(body.nodes)


# Enroll a replacement only after rebuilding its assigned replicas.
@routes.post("/repair/{node}")
def repair_node(node: str, maintenance: Admin):
    return maintenance.repair(node)


# Return a portable snapshot that the caller can save outside the cluster.
@routes.get("/backup")
def export_backup(maintenance: Admin):
    return maintenance.backup()


# Import a snapshot through the durable restore workflow.
@routes.post("/restore")
def restore_backup(body: Snapshot, maintenance: Admin):
    return maintenance.restore(body)
