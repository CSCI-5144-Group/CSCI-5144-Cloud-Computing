"""Shared KV request and record formats."""

import json
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class WriteBody(BaseModel):
    value: Any

    # Preserve the original API's JSON value semantics, including null.
    @field_validator("value")
    @classmethod
    def validate_json(cls, value):
        json.dumps(value, allow_nan=False)
        return value


class Record(WriteBody):
    model_config = ConfigDict(strict=True, extra="forbid")

    key: str = Field(min_length=1)
    revision: int = Field(gt=0, le=2**63 - 1)
    deleted: bool = False

    # Keep tombstones distinct from live records whose value is JSON null.
    @model_validator(mode="after")
    def validate_tombstone(self):
        if self.deleted and self.value is not None:
            raise ValueError("A tombstone must have a null value")
        return self


# Compare JSON values with stable object ordering and distinct scalar types.
def same_json(first, second):
    return json.dumps(first, sort_keys=True) == json.dumps(second, sort_keys=True)


# Compare the record metadata and its exact JSON value.
def same_record(first, second):
    return first is not None and same_json(first.model_dump(), second.model_dump())
