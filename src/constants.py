"""Shared project and dataset configuration."""

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATASETS_DIR = PROJECT_ROOT / "datasets"

DATASET_NAME = "hits"
INDEX_COLUMN = "UserID"
INDEX_VALUE = 15985305027620249815
INDEX_COLUMNS = ["UserID", "EventDate"]

COLUMN_VALUE_FORMATS = {
    "WatchID": "uint64",
    "UserID": "uint64",
    "RefererHash": "uint64",
    "URLHash": "uint64",
    "ClientIP": "ipv4",
    "EventDate": "date",
}


def get_parquet_dataset_path(dataset_name: str) -> Path:
    """Return the Parquet path for a dataset name."""
    return DATASETS_DIR / f"{dataset_name}.parquet"


def get_lance_dataset_path(dataset_name: str) -> Path:
    """Return the Lance path for a dataset name."""
    return DATASETS_DIR / f"{dataset_name}.lance"
