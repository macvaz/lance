"""Remove indexes from a Lance dataset."""

import argparse
import time
from collections.abc import Sequence
from pathlib import Path

import lance

from constants import DATASET_NAME, get_lance_dataset_path


def normalize_index_name(name: str) -> str:
    """Normalize an index name or indexed-column name."""
    return name if name.endswith("_idx") else f"{name}_idx"


def list_index_names(dataset: lance.LanceDataset) -> list[str]:
    """Return the names of indexes currently present in a Lance dataset."""
    return [item["name"] for item in dataset.list_indices()]


def drop_indexes(
    dataset_path: Path,
    names: Sequence[str] | None = None,
) -> lance.LanceDataset:
    """Remove selected Lance indexes, or every index when names are omitted."""
    dataset_path = dataset_path.resolve()
    if not dataset_path.exists():
        raise FileNotFoundError(f"Lance dataset not found: {dataset_path}")

    dataset = lance.dataset(dataset_path)
    existing_names = list_index_names(dataset)

    requested_names = (
        [normalize_index_name(name) for name in names]
        if names
        else existing_names
    )

    if not requested_names:
        print("No Lance indexes found.", flush=True)
        return dataset

    operation_started = time.perf_counter()
    print(
        f"Removing {len(requested_names)} Lance index(es) from {dataset_path}...",
        flush=True,
    )

    for position, index_name in enumerate(requested_names, start=1):
        if index_name not in existing_names:
            raise LookupError(f"Index not found: {index_name}")

        index_started = time.perf_counter()
        print(f"[{position}/{len(requested_names)}] Removing '{index_name}'...", flush=True)
        dataset.drop_index(index_name)
        elapsed = time.perf_counter() - index_started
        print(
            f"[{position}/{len(requested_names)}] Removed '{index_name}' in {elapsed:.3f}s",
            flush=True,
        )

    elapsed = time.perf_counter() - operation_started
    print(f"Index removal completed in {elapsed:.3f}s", flush=True)
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
        "--indices",
        nargs="+",
        metavar="INDEX",
        help="Index names or indexed column names; omit to remove every index",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    dataset = drop_indexes(args.dataset, args.indices)
    remaining_names = list_index_names(dataset)
    remaining = ", ".join(remaining_names) if remaining_names else "none"
    print(f"Remaining Lance indexes: {remaining}")


if __name__ == "__main__":
    main()
