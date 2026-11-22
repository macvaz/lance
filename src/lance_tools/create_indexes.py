"""Create BTREE indexes on Lance dataset columns."""

import argparse
import time
from collections.abc import Sequence
from pathlib import Path

import lance

from constants import DATASET_NAME, INDEX_COLUMNS, get_lance_dataset_path


def create_index(
    dataset_path: Path,
    columns: Sequence[str] = INDEX_COLUMNS,
) -> lance.LanceDataset:
    """Create a BTREE index for each requested column."""
    dataset_path = dataset_path.resolve()
    if not dataset_path.exists():
        raise FileNotFoundError(f"Lance dataset not found: {dataset_path}")

    dataset = lance.dataset(dataset_path)
    operation_started = time.perf_counter()
    print(f"Creating {len(columns)} BTREE index(es) in {dataset_path}...", flush=True)

    for position, column in enumerate(columns, start=1):
        index_started = time.perf_counter()
        print(f"[{position}/{len(columns)}] Creating index for '{column}'...", flush=True)
        dataset.create_scalar_index(column, "BTREE")
        elapsed = time.perf_counter() - index_started
        print(f"[{position}/{len(columns)}] Created index for '{column}' in {elapsed:.3f}s", flush=True)

    elapsed = time.perf_counter() - operation_started
    print(f"Index creation completed in {elapsed:.3f}s", flush=True)
    return dataset


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "dataset",
        nargs="?",
        type=Path,
        default=get_lance_dataset_path(DATASET_NAME),
    )
    parser.add_argument(
        "--columns",
        nargs="+",
        default=INDEX_COLUMNS,
        metavar="COLUMN",
        help="Columns to index (default: all columns in INDEX_COLUMNS)",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    dataset = create_index(args.dataset, args.columns)
    columns = ", ".join(args.columns)
    print(f"Created BTREE indexes on '{columns}' at {dataset.uri}")


if __name__ == "__main__":
    main()
