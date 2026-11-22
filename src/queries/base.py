"""Shared query argument and Lance-formatting behavior."""

import argparse
import ipaddress
import re
from datetime import date
from pathlib import Path

import lance

from constants import (
    COLUMN_VALUE_FORMATS,
    DATASET_NAME,
    INDEX_COLUMN,
    INDEX_VALUE,
    get_lance_dataset_path,
)


class QueryBase:
    """Common command-line parsing and Lance result formatting."""

    def build_parser(self) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(description=__doc__)
        parser.add_argument("--lance", nargs="?", const=DATASET_NAME, metavar="DATASET")
        parser.add_argument("--parquet", nargs="?", const=DATASET_NAME, metavar="DATASET")
        parser.add_argument("--indexes", default=INDEX_COLUMN, help="comma-separated index names")
        parser.add_argument("--values", default=str(INDEX_VALUE), metavar="VALUES")
        parser.add_argument("--fields", default="*", help="comma-separated result fields or '*'")
        return parser

    def parse_indexes(self, raw_value: str) -> list[str]:
        indexes = [index.strip() for index in raw_value.split(",") if index.strip()]
        if not indexes:
            raise SystemExit("--indexes must contain at least one column name")
        if len(indexes) != len(set(indexes)):
            raise SystemExit("--indexes must not contain duplicate column names")
        return indexes

    def parse_uint64(self, raw_value: str, column: str) -> int:
        try:
            value = int(raw_value)
        except ValueError as error:
            raise SystemExit(f"Invalid {column} '{raw_value}'. Use an integer value.") from error
        if -(1 << 63) <= value < (1 << 63):
            return value
        if 0 <= value < (1 << 64):
            return value - (1 << 64)
        raise SystemExit(f"Invalid {column} '{raw_value}'. Expected a 64-bit integer.")

    def parse_ipv4(self, raw_value: str) -> int:
        try:
            unsigned = int(ipaddress.IPv4Address(raw_value))
        except ipaddress.AddressValueError as error:
            raise SystemExit(f"Invalid IPv4 address '{raw_value}'.") from error
        return unsigned - (1 << 32) if unsigned >= (1 << 31) else unsigned

    def parse_value(self, raw_value: str) -> object:
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

    def parse_column_values(self, indexes: list[str], raw_values: str) -> dict[str, object]:
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
                column_values[index] = self.parse_ipv4(raw_value)
            elif value_format == "uint64":
                column_values[index] = self.parse_uint64(raw_value, index)
            else:
                column_values[index] = self.parse_value(raw_value)
        return column_values

    def parse_fields(self, raw_value: str) -> list[str] | None:
        if raw_value.strip() == "*":
            return None
        fields = [field.strip() for field in raw_value.split(",") if field.strip()]
        if not fields:
            raise SystemExit("--fields must contain at least one field or '*'")
        return fields

    def format_lance_value(self, value: object) -> str:
        if isinstance(value, str):
            escaped = value.replace("\\", "\\\\").replace("'", "\\'")
            return f"'{escaped}'"
        if isinstance(value, bool):
            return "true" if value else "false"
        return str(value)

    def build_lance_filter(self, column_values: dict[str, object]) -> str:
        conditions = [
            f"`{column}` = {self.format_lance_value(value)}"
            for column, value in column_values.items()
        ]
        return " AND ".join(conditions)

    def format_bytes(self, size_bytes: float) -> str:
        size = float(size_bytes)
        for unit in ("B", "KB", "MB", "GB", "TB"):
            if size < 1000 or unit == "TB":
                return f"{size:.2f} {unit}" if unit != "B" else f"{size:.0f} {unit}"
            size /= 1000
        return f"{size:.2f} TB"

    def parse_metrics(self, metrics_text: str) -> list[tuple[str, str]]:
        content = metrics_text.strip()[1:-1]
        parsed: list[tuple[str, str]] = []
        for item in content.split(","):
            if "=" in item:
                name, value = item.split("=", 1)
                parsed.append((name.strip(), value.strip()))
        return parsed

    def print_metrics_table(self, section_name: str, metrics: list[tuple[str, str]]) -> None:
        name_width = max(len("Metric"), *(len(name) for name, _ in metrics))
        print(f"\n{section_name}", flush=True)
        print(f"{'Metric':<{name_width}}  Value", flush=True)
        print(f"{'-' * name_width}  {'-' * 18}", flush=True)
        for name, value in metrics:
            print(f"{name:<{name_width}}  {value}", flush=True)

    def print_lance_analysis(
        self,
        dataset_path: Path,
        column_values: dict[str, object],
        fields: list[str] | None = None,
    ) -> None:
        lance_filter = self.build_lance_filter(column_values)
        dataset = lance.dataset(dataset_path)
        scanner = (
            dataset.scanner(filter=lance_filter)
            if fields is None
            else dataset.scanner(filter=lance_filter, columns=fields)
        )
        analysis = scanner.analyze_plan()

        print(f"Lance filter: {lance_filter}", flush=True)
        print("Lance performance metrics:", flush=True)

        lance_read_match = re.search(r"LanceRead:.*?metrics=(\[[^\n]*\])", analysis, flags=re.DOTALL)
        if lance_read_match:
            self.print_metrics_table("LanceRead", self.parse_metrics(lance_read_match.group(1)))

        scalar_match = re.search(r"ScalarIndexQuery:.*?metrics=(\[[^\n]*\])", analysis, flags=re.DOTALL)
        if scalar_match:
            scalar_metrics = dict(self.parse_metrics(scalar_match.group(1)))
            self.print_metrics_table("ScalarIndexQuery", [
                ("indexed", "yes"),
                ("output_rows", scalar_metrics.get("output_rows", "0")),
                ("output_bytes", scalar_metrics.get("output_bytes", "0 B")),
                ("search_time", scalar_metrics.get("search_time", "0s")),
            ])
        else:
            self.print_metrics_table("ScalarIndexQuery", [("indexed", "no")])

    def get_lance_path(self, dataset_name: str) -> Path:
        return get_lance_dataset_path(dataset_name)
