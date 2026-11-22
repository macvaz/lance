"""Models for Lance query responses."""

from typing import Any

from pydantic import BaseModel


class QueryResponse(BaseModel):
    dataset: str
    lance_filter: str
    execution_time_ms: float
    row_count: int
    column_count: int
    estimated_size_bytes: int
    columns: list[str]
    rows: list[dict[str, Any]]
