"""Query Lance datasets using Lance directly and no Polars or DuckDB dependency.

Example:
    uv run query_native --indexes UserID,EventDate --values "15985305027620249815,2013-07-15" --fields WatchID,EventDate
"""

import time
from pathlib import Path
from typing import Any

import lance

from constants import DATASET_NAME
from queries.base import QueryBase


class NativeQuery(QueryBase):
    """Execute scalar lookups using Lance directly."""

    def query_lance(
        self,
        dataset_path: Path,
        column_values: dict[str, object],
        indexes: list[str],
        fields: list[str] | None,
    ) -> tuple[Any, float]:
        dataset = lance.dataset(dataset_path)
        lance_filter = self.build_lance_filter(column_values)
        scanner_columns = None if fields is None else list(dict.fromkeys([*fields, *indexes]))

        started_at = time.perf_counter()
        order_by: list[Any] = list(indexes)
        scanner = (
            dataset.scanner(filter=lance_filter, order_by=order_by)
            if scanner_columns is None
            else dataset.scanner(filter=lance_filter, columns=scanner_columns, order_by=order_by)
        )
        table = scanner.to_table()
        if fields is not None:
            table = table.select(fields)

        elapsed = time.perf_counter() - started_at
        return table, elapsed

    def format_read_details(self, dataset_path: Path, table: Any, elapsed: float) -> None:
        print(f"Dataset (Lance): {dataset_path}", flush=True)
        print(f"Execution time (Lance): {elapsed:.3f}s", flush=True)
        print(f"Rows (Lance): {table.num_rows}", flush=True)
        print(f"Columns (Lance): {table.num_columns}", flush=True)
        print(f"Estimated data size (Lance): {self.format_bytes(table.nbytes)}", flush=True)

    def main(self) -> None:
        args = self.build_parser().parse_args()
        indexes = self.parse_indexes(args.indexes)
        fields = self.parse_fields(args.fields)
        column_values = self.parse_column_values(indexes, args.values)
        print(column_values, flush=True)

        dataset_name = args.lance if args.lance is not None else DATASET_NAME
        dataset_path = self.get_lance_path(dataset_name)
        if not dataset_path.exists():
            raise SystemExit(f"Lance dataset not found: {dataset_path.resolve()}")

        table, elapsed = self.query_lance(dataset_path, column_values, indexes, fields)
        self.format_read_details(dataset_path, table, elapsed)
        self.print_lance_analysis(dataset_path, column_values, fields)
        print(table)


def main() -> None:
    NativeQuery().main()


if __name__ == "__main__":
    main()
