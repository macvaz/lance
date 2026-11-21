"""Look up ClickBench hits rows by WatchID and compare Lance with Parquet.

WatchID is the primary key. Polars reads both files with the same filter and
checks that the returned rows match. The Lance btree is created by
convert_to_lance.py.

    uv run python lookup_hits.py
    uv run python lookup_hits.py --n 20
    uv run python lookup_hits.py --pks 1,2,3
"""

import argparse
import random
import time
from pathlib import Path

import lance
import polars as pl

PK = "WatchID"


def sample_pks(dataset: lance.LanceDataset, n: int, seed: int) -> list[int]:
    """Pick primary keys with positional reads, without scanning the table."""
    rng = random.Random(seed)
    nrows = dataset.count_rows()
    if n > nrows:
        raise SystemExit(f"--n {n} is larger than the dataset ({nrows:,} rows)")
    offsets = rng.sample(range(nrows), n)
    values = dataset.take(offsets, columns=[PK]).column(PK).to_pylist()
    return [int(value) for value in values]


def lookup(scan, path: Path, pks: list[int]) -> tuple[pl.DataFrame, float]:
    started = time.perf_counter()
    frame = scan(path).filter(pl.col(PK).is_in(pks)).collect()
    return frame, time.perf_counter() - started


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lance", default="hits.lance", help="Lance dataset")
    parser.add_argument("--parquet", default="hits.parquet", help="Parquet file")
    parser.add_argument("--n", type=int, default=5, help="How many random PKs to read")
    parser.add_argument(
        "--pks",
        help="Comma-separated WatchID values. Skips random sampling.",
    )
    parser.add_argument("--seed", type=int, default=0)
    args = parser.parse_args()

    lance_path = Path(args.lance)
    parquet_path = Path(args.parquet)
    if not lance_path.exists():
        raise SystemExit(f"Lance dataset not found: {lance_path.resolve()}")
    if not parquet_path.is_file():
        raise SystemExit(f"Parquet file not found: {parquet_path.resolve()}")

    dataset = lance.dataset(lance_path)
    if args.pks:
        pks = [int(value) for value in args.pks.split(",") if value.strip()]
    else:
        pks = sample_pks(dataset, args.n, args.seed)
    print(f"{PK}: {pks}", flush=True)

    lance_df, lance_seconds = lookup(pl.scan_lance, lance_path, pks)
    parquet_df, parquet_seconds = lookup(pl.scan_parquet, parquet_path, pks)

    lance_rows = lance_df.sort(PK)
    parquet_rows = parquet_df.sort(PK)
    print(
        f"Lance   {lance_seconds:8.3f}s  {lance_rows.height} rows\n"
        f"Parquet {parquet_seconds:8.3f}s  {parquet_rows.height} rows",
        flush=True,
    )
    if not lance_rows.equals(parquet_rows):
        print("Rows differ.", flush=True)
        print(lance_rows)
        print(parquet_rows)
        raise SystemExit(1)

    print(lance_rows)


if __name__ == "__main__":
    main()
