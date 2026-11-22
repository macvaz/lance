"""Reusable helpers for locating and inspecting Lance datasets."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from fastapi import HTTPException

from constants import DATASETS_DIR


class DatasetManager:
    """Manage dataset names, paths, schemas, and projections."""

    DATASET_NAME_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")

    def __init__(self, query_engine: Any) -> None:
        self.query_engine = query_engine

    def get_path(self, dataset_name: str) -> Path:
        if not self.DATASET_NAME_PATTERN.fullmatch(dataset_name):
            raise HTTPException(status_code=400, detail="Invalid dataset name")
        path = DATASETS_DIR / f"{dataset_name}.lance"
        if not path.is_dir():
            raise HTTPException(status_code=404, detail=f"Lance dataset not found: {dataset_name}")
        return path

    def list_names(self) -> list[str]:
        return sorted(
            path.name.removesuffix(".lance")
            for path in DATASETS_DIR.glob("*.lance")
            if path.is_dir()
        )

    def validate_columns(self, dataset: Any, requested: list[str] | None) -> list[str] | None:
        if requested is None:
            return None
        available = set(dataset.schema.names)
        unknown = [column for column in requested if column not in available]
        if unknown:
            raise HTTPException(status_code=422, detail=f"Unknown columns: {', '.join(unknown)}")
        return list(dict.fromkeys(requested))

    def get_schema(self, dataset_name: str) -> dict[str, Any]:
        path = self.get_path(dataset_name)
        dataset = self.query_engine.get_dataset(path)
        fields = [
            {"name": field.name, "type": str(field.type), "nullable": field.nullable}
            for field in dataset.schema
        ]
        return {"dataset": dataset_name, "path": str(path), "fields": fields}
