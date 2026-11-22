"""Compare Lance and Parquet scalar lookups with Polars."""

import time
from collections.abc import Callable
from pathlib import Path

import polars as pl

from constants import DATASET_NAME, get_lance_dataset_path, get_parquet_dataset_path
from queries.base import QueryBase


class PolarsQuery(QueryBase):
    """Execute and compare Lance and Parquet lookups with Polars."""

    def query(
        self,
        scanner: Callable[[Path], pl.LazyFrame],
        dataset_path: Path,
        column_values: dict[str, object],
        indexes: list[str],
        fields: list[str] | None = None,
    ) -> tuple[pl.DataFrame, float]:
        started_at = time.perf_counter()
        frame_query = scanner(dataset_path)
        for column, value in column_values.items():
            frame_query = frame_query.filter(pl.col(column) == value)
        frame_query = frame_query.sort(indexes)
        if fields is not None:
            frame_query = frame_query.select([*fields])

        frame = frame_query.collect()
        elapsed = time.perf_counter() - started_at
        return frame, elapsed

    def format_read_details(
        self,
        name: str,
        dataset_path: Path,
        frame: pl.DataFrame,
        elapsed: float,
    ) -> None:
        print(f"Dataset ({name}): {dataset_path}", flush=True)
        print(f"Execution time ({name}): {elapsed:.3f}s", flush=True)
        print(f"Rows ({name}): {frame.height}", flush=True)
        print(f"Columns ({name}): {frame.width}", flush=True)
        print(f"Estimated data size ({name}): {self.format_bytes(frame.estimated_size('b'))}", flush=True)

    def main(self) -> None:
        args = self.build_parser().parse_args()
        indexes = self.parse_indexes(args.indexes)
        fields = self.parse_fields(args.fields)
        column_values = self.parse_column_values(indexes, args.values)
        print(column_values, flush=True)

        selected_formats: list[tuple[str, Path, Callable[[Path], pl.LazyFrame]]] = []
        if args.lance is not None:
            lance_path = get_lance_dataset_path(args.lance)
            if not lance_path.exists():
                raise SystemExit(f"Lance dataset not found: {lance_path.resolve()}")
            selected_formats.append(("Lance", lance_path, pl.scan_lance))
        if args.parquet is not None:
            parquet_path = get_parquet_dataset_path(args.parquet)
            if not parquet_path.is_file():
                raise SystemExit(f"Parquet file not found: {parquet_path.resolve()}")
            selected_formats.append(("Parquet", parquet_path, pl.scan_parquet))
        if not selected_formats:
            lance_path = get_lance_dataset_path(DATASET_NAME)
            if not lance_path.exists():
                raise SystemExit(f"Lance dataset not found: {lance_path.resolve()}")
            selected_formats.append(("Lance", lance_path, pl.scan_lance))

        results: dict[str, tuple[pl.DataFrame, float]] = {}
        for name, dataset_path, scanner in selected_formats:
            frame, elapsed = self.query(scanner, dataset_path, column_values, indexes, fields)
            results[name] = (frame, elapsed)
            self.format_read_details(name, dataset_path, frame, elapsed)
            if name == "Lance":
                self.print_lance_analysis(dataset_path, column_values, fields)

        if len(results) == 2:
            lance_frame = results["Lance"][0]
            parquet_frame = results["Parquet"][0]
            if not lance_frame.equals(parquet_frame):
                print("Rows differ.", flush=True)
                print(lance_frame)
                print(parquet_frame)
                raise SystemExit(1)

        for frame, _ in results.values():
            print(frame)


def main() -> None:
    PolarsQuery().main()


if __name__ == "__main__":
    main()
