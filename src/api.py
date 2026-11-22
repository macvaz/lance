"""REST API for querying Lance datasets."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

import uvicorn
from fastapi import FastAPI, HTTPException, Query

from datasets import DatasetManager
from models import QueryResponse
from queries.native import NativeQuery

native_query = NativeQuery()
dataset_manager = DatasetManager(native_query)
startup_warmup: dict[str, Any] = {"status": "not_started", "datasets": []}


@asynccontextmanager
async def lifespan(_: FastAPI):
    """Prewarm all Lance indexes before the API starts accepting requests."""
    datasets: list[dict[str, Any]] = []

    for dataset_name in dataset_manager.list_names():
        path = dataset_manager.get_path(dataset_name)
        warmed, elapsed = native_query.warm_up_indexes(path)
        datasets.append(
            {
                "dataset": dataset_name,
                "warmed_indexes": warmed,
                "warm_up_time_ms": elapsed * 1000,
            }
        )

    startup_warmup.update(status="completed", datasets=datasets)
    yield


api = FastAPI(
    title="Lance Dataset API",
    description="Schema discovery and indexed lookups for Lance datasets.",
    version="0.1.0",
    lifespan=lifespan,
)


@api.get("/health")
def health() -> dict[str, Any]:
    return {"status": "ok", "warmup": startup_warmup}


@api.get("/datasets")
def list_datasets() -> dict[str, list[str]]:
    return {"datasets": dataset_manager.list_names()}


@api.get("/datasets/{dataset_name}/schema")
def dataset_schema(dataset_name: str) -> dict[str, Any]:
    return dataset_manager.get_schema(dataset_name)


def execute_query(
    dataset_name: str,
    index_names: list[str],
    raw_values: list[str],
    requested_columns: list[str] | None,
    limit: int,
) -> QueryResponse:
    path = dataset_manager.get_path(dataset_name)
    dataset = native_query.get_dataset(path)

    for column in index_names:
        if column not in dataset.schema.names:
            raise HTTPException(status_code=422, detail=f"Unknown condition column: {column}")

    column_values: dict[str, object] = {}
    for column, raw_value in zip(index_names, raw_values, strict=True):
        if not raw_value:
            raise HTTPException(status_code=422, detail=f"The value for '{column}' must not be empty")
        try:
            column_values[column] = native_query.parse_column_value(column, raw_value)
        except SystemExit as error:
            raise HTTPException(status_code=422, detail=str(error)) from error
    lance_filter = native_query.build_lance_filter(column_values)
    columns = dataset_manager.validate_columns(dataset, requested_columns)

    table, elapsed = native_query.query_lance(
        dataset_path=path,
        column_values=column_values,
        indexes=index_names,
        fields=columns,
        limit=limit,
    )

    return QueryResponse(
        dataset=dataset_name,
        lance_filter=lance_filter,
        execution_time_ms=elapsed * 1000,
        row_count=table.num_rows,
        column_count=table.num_columns,
        estimated_size_bytes=table.nbytes,
        columns=table.column_names,
        rows=table.to_pylist(),
    )


@api.get("/datasets/{dataset_name}/query")
def query_dataset_get(
    dataset_name: str,
    indexes: str = Query(..., description="Comma-separated condition columns"),
    values: str = Query(..., description="Comma-separated values in index-column order"),
    fields: str = Query("*", description="Comma-separated returned columns, or '*'"),
    limit: int = Query(100, ge=1, le=10_000),
) -> QueryResponse:
    index_names = [item.strip() for item in indexes.split(",") if item.strip()]
    raw_values = [item.strip() for item in values.split(",") if item.strip()]
    if not index_names:
        raise HTTPException(status_code=422, detail="indexes must contain at least one column")
    if len(index_names) != len(raw_values):
        raise HTTPException(status_code=422, detail="indexes and values must contain the same number of items")
    if len(index_names) != len(set(index_names)):
        raise HTTPException(status_code=422, detail="indexes must not contain duplicate columns")

    requested_columns = None
    if fields.strip() != "*":
        requested_columns = [item.strip() for item in fields.split(",") if item.strip()]
        if not requested_columns:
            raise HTTPException(status_code=422, detail="fields must contain column names or '*'")

    return execute_query(
        dataset_name,
        index_names,
        raw_values,
        requested_columns,
        limit,
    )


def main() -> None:
    uvicorn.run("api:api", host="127.0.0.1", port=8000)


if __name__ == "__main__":
    main()
