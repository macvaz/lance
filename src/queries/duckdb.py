"""Compare Lance and Parquet scalar lookups with DuckDB."""

import time
from collections.abc import Callable
from pathlib import Path

import duckdb
import lance
import pyarrow as pa

from constants import DATASET_NAME, get_lance_dataset_path, get_parquet_dataset_path
from queries.base import QueryBase


class DuckDBQuery(QueryBase):
    """Execute and compare Lance and Parquet lookups with DuckDB."""

    def quote_identifier(self, identifier: str) -> str:
        return '"' + identifier.replace('"', '""') + '"'

    def format_sql_value(self, value: object) -> str:
        if isinstance(value, str):
            return "'" + value.replace("'", "''") + "'"
        if isinstance(value, bool):
            return "true" if value else "false"
        return str(value)

    def query_lance(
        self,
        dataset_path: Path,
        column_values: dict[str, object],
        indexes: list[str],
        fields: list[str] | None = None,
    ) -> tuple[pa.Table, float]:
        connection = duckdb.connect()
        dataset = lance.dataset(dataset_path)
        lance_filter = self.build_lance_filter(column_values)
        scanner_columns = fields
        if fields is not None:
            scanner_columns = list(dict.fromkeys([*fields, *indexes]))

        started_at = time.perf_counter()
        scanner = (
            dataset.scanner(filter=lance_filter)
            if scanner_columns is None
            else dataset.scanner(filter=lance_filter, columns=scanner_columns)
        )
        table = scanner.to_table()

        connection.register("lance_result", table)
        projection = "*" if fields is None else ", ".join(self.quote_identifier(field) for field in fields)
        order_by = ", ".join(self.quote_identifier(index) for index in indexes)
        sql = f"SELECT {projection} FROM lance_result ORDER BY {order_by}"
        table = connection.sql(sql).to_arrow_table()
        elapsed = time.perf_counter() - started_at
        connection.close()
        return table, elapsed

    def query_parquet(
        self,
        dataset_path: Path,
        column_values: dict[str, object],
        indexes: list[str],
        fields: list[str] | None = None,
    ) -> tuple[pa.Table, float]:
        connection = duckdb.connect()
        projection = "*" if fields is None else ", ".join(self.quote_identifier(field) for field in fields)
        conditions = " AND ".join(
            f"{self.quote_identifier(column)} = {self.format_sql_value(value)}"
            for column, value in column_values.items()
        )
        order_by = ", ".join(self.quote_identifier(index) for index in indexes)
        escaped_path = dataset_path.resolve().as_posix().replace("'", "''")
        sql = (
            f"SELECT {projection} FROM read_parquet('{escaped_path}') "
            f"WHERE {conditions} ORDER BY {order_by}"
        )

        started_at = time.perf_counter()
        table = connection.sql(sql).to_arrow_table()
        elapsed = time.perf_counter() - started_at
        connection.close()
        return table, elapsed

    def format_read_details(
        self,
        name: str,
        dataset_path: Path,
        table: pa.Table,
        elapsed: float,
    ) -> None:
        print(f"Dataset ({name}): {dataset_path}", flush=True)
        print(f"Execution time ({name}): {elapsed:.3f}s", flush=True)
        print(f"Rows ({name}): {table.num_rows}", flush=True)
        print(f"Columns ({name}): {table.num_columns}", flush=True)
        print(f"Estimated data size ({name}): {self.format_bytes(table.nbytes)}", flush=True)

    def main(self) -> None:
        args = self.build_parser().parse_args()
        indexes = self.parse_indexes(args.indexes)
        fields = self.parse_fields(args.fields)
        column_values = self.parse_column_values(indexes, args.values)
        print(column_values, flush=True)

        selected_formats: list[tuple[str, Path, Callable[..., tuple[pa.Table, float]]]] = []
        if args.lance is not None:
            lance_path = get_lance_dataset_path(args.lance)
            if not lance_path.exists():
                raise SystemExit(f"Lance dataset not found: {lance_path.resolve()}")
            selected_formats.append(("Lance", lance_path, self.query_lance))
        if args.parquet is not None:
            parquet_path = get_parquet_dataset_path(args.parquet)
            if not parquet_path.is_file():
                raise SystemExit(f"Parquet file not found: {parquet_path.resolve()}")
            selected_formats.append(("Parquet", parquet_path, self.query_parquet))
        if not selected_formats:
            lance_path = get_lance_dataset_path(DATASET_NAME)
            if not lance_path.exists():
                raise SystemExit(f"Lance dataset not found: {lance_path.resolve()}")
            selected_formats.append(("Lance", lance_path, self.query_lance))

        results: dict[str, tuple[pa.Table, float]] = {}
        for name, dataset_path, query_function in selected_formats:
            table, elapsed = query_function(dataset_path, column_values, indexes, fields)
            results[name] = (table, elapsed)
            self.format_read_details(name, dataset_path, table, elapsed)
            if name == "Lance":
                self.print_lance_analysis(dataset_path, column_values, fields)

        if len(results) == 2:
            lance_table = results["Lance"][0]
            parquet_table = results["Parquet"][0]
            if lance_table.to_pylist() != parquet_table.to_pylist():
                print("Rows differ.", flush=True)
                print(lance_table)
                print(parquet_table)
                raise SystemExit(1)

        for table, _ in results.values():
            print(table)


def main() -> None:
    DuckDBQuery().main()


if __name__ == "__main__":
    main()
