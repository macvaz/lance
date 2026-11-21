"""Stream the ClickBench hits Parquet file into a Lance dataset.

The file is about 15 GB and 100 million rows, so this reads it as a PyArrow
dataset and lets Lance pull record batches. It does not load the table into
memory. After the write it builds a btree on WatchID, the primary key.

    uv run python convert_to_lance.py hits.parquet
"""

import os

os.environ["POLARS_MAX_THREADS"] = "2"  # antes del import

import argparse
import time
from pathlib import Path

import lance
import pyarrow.dataset as ds

PK = "WatchID"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "parquet",
        nargs="?",
        default="hits.parquet",
        help="Path to hits.parquet (default: ./hits.parquet)",
    )
    parser.add_argument(
        "--out",
        default="hits.lance",
        help="Lance dataset directory to create (default: ./hits.lance)",
    )
    args = parser.parse_args()

    source = Path(args.parquet)
    if not source.is_file():
        raise SystemExit(
            f"Parquet file not found: {source.resolve()}\n"
            "Wait until the download finishes, then pass that path."
        )

    parquet = ds.dataset(source, format="parquet")
    rows = parquet.count_rows()
    print(
        f"Reading {source} ({rows:,} rows, {len(parquet.schema)} columns)",
        flush=True,
    )

    dataset = lance.write_dataset(
        parquet,
        args.out,
        mode="overwrite",
        max_rows_per_file=1_000_000,
    )
    print(
        f"Wrote {dataset.uri} ({dataset.count_rows():,} rows)",
        flush=True,
    )

    print(f"Building a btree on {PK}", flush=True)
    started = time.perf_counter()
    dataset.create_scalar_index(PK, "BTREE")
    print(f"Index built in {time.perf_counter() - started:.1f}s", flush=True)


if __name__ == "__main__":
    main()
