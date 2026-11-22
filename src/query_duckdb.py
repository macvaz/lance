"""Compare Lance and Parquet scalar lookups with DuckDB.

Example:
    uv run query_duckdb --indexes UserID,EventDate --values "15985305027620249815,2013-07-15" --fields WatchID,EventDate
"""

import argparse
import ipaddress
import re
import time
from collections.abc import Callable
from datetime import date
from pathlib import Path

import duckdb
import lance
import pyarrow as pa

from constants import (
    COLUMN_VALUE_FORMATS,
    DATASET_NAME,
    INDEX_COLUMN,
    INDEX_VALUE,
    get_lance_dataset_path,
    get_parquet_dataset_path,
)


def query_lance(
    dataset_path: Path,
    column_values: dict[str, object],
    indexes: list[str],
    fields: list[str] | None = None,
) -> tuple[pa.Table, float]:
    """Run a Lance scalar lookup through Lance native filtering and DuckDB result sorting."""
    connection = duckdb.connect()
    dataset = lance.dataset(dataset_path)
    lance_filter = build_lance_filter(column_values)

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
    projection = "*" if fields is None else ", ".join(quote_identifier(field) for field in fields)
    order_by = ", ".join(quote_identifier(index) for index in indexes)
    sql = f"SELECT {projection} FROM lance_result ORDER BY {order_by}"
    table = connection.sql(sql).to_arrow_table()

    elapsed = time.perf_counter() - started_at
    connection.close()
    return table, elapsed


def query_parquet(
    dataset_path: Path,
    column_values: dict[str, object],
    indexes: list[str],
    fields: list[str] | None = None,
) -> tuple[pa.Table, float]:
    """Run a Parquet scalar lookup with DuckDB."""
    connection = duckdb.connect()
    projection = "*" if fields is None else ", ".join(quote_identifier(field) for field in fields)
    conditions = " AND ".join(
        f"{quote_identifier(column)} = {format_sql_value(value)}"
        for column, value in column_values.items()
    )
    order_by = ", ".join(quote_identifier(index) for index in indexes)
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


def parse_indexes(raw_value: str) -> list[str]:
    indexes = [index.strip() for index in raw_value.split(",") if index.strip()]
    if not indexes:
        raise SystemExit("--indexes must contain at least one column name")
    if len(indexes) != len(set(indexes)):
        raise SystemExit("--indexes must not contain duplicate column names")
    return indexes


def parse_uint64(raw_value: str, column: str) -> int:
    try:
        value = int(raw_value)
    except ValueError as error:
        raise SystemExit(f"Invalid {column} '{raw_value}'. Use an integer value.") from error
    if -(1 << 63) <= value < (1 << 63):
        return value
    if 0 <= value < (1 << 64):
        return value - (1 << 64)
    raise SystemExit(f"Invalid {column} '{raw_value}'. Expected a 64-bit integer.")


def parse_ipv4(raw_value: str) -> int:
    try:
        unsigned = int(ipaddress.IPv4Address(raw_value))
    except ipaddress.AddressValueError as error:
        raise SystemExit(f"Invalid IPv4 address '{raw_value}'.") from error
    return unsigned - (1 << 32) if unsigned >= (1 << 31) else unsigned


def parse_value(raw_value: str) -> object:
    lowered = raw_value.lower()
    if lowered in {"true", "false"}:
        return lowered == "true"
    try:
        return int(raw_value)
    except ValueError:
        try:
            return float(raw_value)
        except ValueError:
            return raw_value


def parse_column_values(indexes: list[str], raw_values: str) -> dict[str, object]:
    raw_values_list = [value.strip() for value in raw_values.split(",") if value.strip()]
    if len(indexes) != len(raw_values_list):
        raise SystemExit("--indexes and --values must contain the same number of items")

    column_values: dict[str, object] = {}
    for index, raw_value in zip(indexes, raw_values_list, strict=True):
        if not raw_value:
            raise SystemExit(f"The value for '{index}' must not be empty")
        value_format = COLUMN_VALUE_FORMATS.get(index)
        if value_format == "date":
            try:
                event_date = date.fromisoformat(raw_value)
            except ValueError as error:
                raise SystemExit(f"Invalid {index} '{raw_value}'. Use YYYY-MM-DD format.") from error
            column_values[index] = (event_date - date(1970, 1, 1)).days
        elif value_format == "ipv4":
            column_values[index] = parse_ipv4(raw_value)
        elif value_format == "uint64":
            column_values[index] = parse_uint64(raw_value, index)
        else:
            column_values[index] = parse_value(raw_value)
    return column_values


def parse_fields(raw_value: str) -> list[str] | None:
    if raw_value.strip() == "*":
        return None
    fields = [field.strip() for field in raw_value.split(",") if field.strip()]
    if not fields:
        raise SystemExit("--fields must contain at least one field or '*'")
    return fields


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lance", nargs="?", const=DATASET_NAME, metavar="DATASET")
    parser.add_argument("--parquet", nargs="?", const=DATASET_NAME, metavar="DATASET")
    parser.add_argument("--indexes", default=INDEX_COLUMN, help="comma-separated index names")
    parser.add_argument("--values", default=str(INDEX_VALUE), metavar="VALUES")
    parser.add_argument("--fields", default="*", help="comma-separated result fields or '*'")
    return parser


def quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def format_sql_value(value: object) -> str:
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def format_bytes(size_bytes: float) -> str:
    size = float(size_bytes)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1000 or unit == "TB":
            return f"{size:.2f} {unit}" if unit != "B" else f"{size:.0f} {unit}"
        size /= 1000
    return f"{size:.2f} TB"


def format_read_details(name: str, dataset_path: Path, table: pa.Table, elapsed: float) -> None:
    print(f"Dataset ({name}): {dataset_path}", flush=True)
    print(f"Execution time ({name}): {elapsed:.3f}s", flush=True)
    print(f"Rows ({name}): {table.num_rows}", flush=True)
    print(f"Columns ({name}): {table.num_columns}", flush=True)
    print(f"Estimated data size ({name}): {format_bytes(table.nbytes)}", flush=True)


def build_lance_filter(column_values: dict[str, object]) -> str:
    conditions = [
        f"`{column}` = {format_lance_value(value)}"
        for column, value in column_values.items()
    ]
    return " AND ".join(conditions)


def format_lance_value(value: object) -> str:
    if isinstance(value, str):
        escaped = value.replace("\\", "\\\\").replace("'", "\\'")
        return f"'{escaped}'"
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def parse_metrics(metrics_text: str) -> list[tuple[str, str]]:
    content = metrics_text.strip()[1:-1]
    parsed: list[tuple[str, str]] = []
    for item in content.split(","):
        if "=" in item:
            name, value = item.split("=", 1)
            parsed.append((name.strip(), value.strip()))
    return parsed


def print_metrics_table(section_name: str, metrics: list[tuple[str, str]]) -> None:
    name_width = max(len("Metric"), *(len(name) for name, _ in metrics))
    print(f"\n{section_name}", flush=True)
    print(f"{'Metric':<{name_width}}  Value", flush=True)
    print(f"{'-' * name_width}  {'-' * 18}", flush=True)
    for name, value in metrics:
        print(f"{name:<{name_width}}  {value}", flush=True)


def print_lance_analysis(dataset_path: Path, column_values: dict[str, object], fields: list[str] | None = None) -> None:
    lance_filter = build_lance_filter(column_values)
    dataset = lance.dataset(dataset_path)
    scanner = dataset.scanner(filter=lance_filter) if fields is None else dataset.scanner(filter=lance_filter, columns=fields)
    analysis = scanner.analyze_plan()

    print(f"Lance filter: {lance_filter}", flush=True)
    print("Lance performance metrics:", flush=True)
    lance_read_match = re.search(r"LanceRead:.*?metrics=(\[[^\n]*\])", analysis, flags=re.DOTALL)
    if lance_read_match:
        print_metrics_table("LanceRead", parse_metrics(lance_read_match.group(1)))

    scalar_match = re.search(r"ScalarIndexQuery:.*?metrics=(\[[^\n]*\])", analysis, flags=re.DOTALL)
    if scalar_match:
        scalar_metrics = dict(parse_metrics(scalar_match.group(1)))
        print_metrics_table("ScalarIndexQuery", [
            ("indexed", "yes"),
            ("output_rows", scalar_metrics.get("output_rows", "0")),
            ("output_bytes", scalar_metrics.get("output_bytes", "0 B")),
            ("search_time", scalar_metrics.get("search_time", "0s")),
        ])
    else:
        print_metrics_table("ScalarIndexQuery", [("indexed", "no")])


def main() -> None:
    args = build_parser().parse_args()
    indexes = parse_indexes(args.indexes)
    fields = parse_fields(args.fields)
    column_values = parse_column_values(indexes, args.values)
    print(column_values, flush=True)

    selected_formats: list[tuple[str, Path, Callable]] = []
    if args.lance is not None:
        lance_path = get_lance_dataset_path(args.lance)
        if not lance_path.exists():
            raise SystemExit(f"Lance dataset not found: {lance_path.resolve()}")
        selected_formats.append(("Lance", lance_path, query_lance))
    if args.parquet is not None:
        parquet_path = get_parquet_dataset_path(args.parquet)
        if not parquet_path.is_file():
            raise SystemExit(f"Parquet file not found: {parquet_path.resolve()}")
        selected_formats.append(("Parquet", parquet_path, query_parquet))
    if not selected_formats:
        lance_path = get_lance_dataset_path(DATASET_NAME)
        if not lance_path.exists():
            raise SystemExit(f"Lance dataset not found: {lance_path.resolve()}")
        selected_formats.append(("Lance", lance_path, query_lance))

    results: dict[str, tuple[pa.Table, float]] = {}
    for name, dataset_path, query_function in selected_formats:
        table, elapsed = query_function(dataset_path, column_values, indexes, fields)
        results[name] = (table, elapsed)
        format_read_details(name, dataset_path, table, elapsed)
        if name == "Lance":
            print_lance_analysis(dataset_path, column_values, fields)

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


if __name__ == "__main__":
    main()
