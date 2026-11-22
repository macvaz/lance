"""Convert a Parquet dataset to Lance."""

import argparse
import time
from collections.abc import Iterator
from pathlib import Path

import lance
import pyarrow as pa
import pyarrow.parquet as pq

from constants import (
    DATASET_NAME,
    get_lance_dataset_path,
    get_parquet_dataset_path,
)


def iter_batches(parquet_file: pq.ParquetFile) -> Iterator[pa.RecordBatch]:
    """Yield record batches while reporting conversion progress."""
    total_rows = parquet_file.metadata.num_rows
    processed_rows = 0

    for batch_number, batch in enumerate(
        parquet_file.iter_batches(batch_size=32_768, use_threads=False)
    ):
        processed_rows += batch.num_rows
        if batch_number % 100 == 0:
            print(
                f"{processed_rows:,}/{total_rows:,} ({processed_rows / total_rows:.1%})",
                flush=True,
            )
            time.sleep(0.5)
        yield batch


def convert_from_parquet(source: Path) -> lance.LanceDataset:
    """Convert a Parquet file to a Lance dataset."""
    source = source.resolve()
    if not source.is_file():
        raise FileNotFoundError(f"Parquet source not found: {source}")

    parquet_file = pq.ParquetFile(source, pre_buffer=False, memory_map=False)
    reader = pa.RecordBatchReader.from_batches(
        parquet_file.schema_arrow,
        iter_batches(parquet_file),
    )

    destination = get_lance_dataset_path(DATASET_NAME).resolve()
    return lance.write_dataset(
        reader,
        destination,
        schema=parquet_file.schema_arrow,
        mode="create",
        max_rows_per_file=2_000_000,
        max_rows_per_group=32_768,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "source",
        nargs="?",
        type=Path,
        default=get_parquet_dataset_path(DATASET_NAME),
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()

    pa.set_cpu_count(2)
    pa.set_io_thread_count(2)

    dataset = convert_from_parquet(args.source)
    print(f"Created Lance dataset at {dataset.uri}")


if __name__ == "__main__":
    main()
