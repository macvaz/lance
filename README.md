# Lance tools

The shared dataset configuration is in `src/constants.py`:

- `DATASET_NAME` selects the default dataset name (`hits`).
- `INDEX_COLUMNS` lists the configured BTREE index columns (`UserID`, `EventDate`, `ClientIP`, `RegionID`).
- `get_parquet_dataset_path(name)` returns `datasets/<name>.parquet`.
- `get_lance_dataset_path(name)` returns `datasets/<name>.lance`.

## Convert Parquet to Lance

```bash
uv run convert_from_parquet
```

`convert_from_parquet` accepts one optional positional source path. By default, it
uses `datasets/hits.parquet` and creates `datasets/hits.lance`:

```bash
uv run convert_from_parquet datasets/hits.parquet
```

The conversion reports batch progress and writes the Lance dataset with 2,000,000
maximum rows per data file and 32,768 maximum rows per row group.

## Create Lance indexes

```bash
uv run create_indexes
```

`create_indexes` accepts an optional Lance dataset path and `--columns`:

```bash
uv run create_indexes datasets/hits.lance --columns WatchID EventTime
```

- Positional `dataset` defaults to `datasets/hits.lance`.
- `--columns` accepts one or more column names separated by spaces.
- Omitting `--columns` creates BTREE indexes for `UserID`, `EventDate`, `ClientIP`,
  and `RegionID`.

## Remove Lance indexes

```bash
uv run drop_indexes
```

`drop_indexes` accepts an optional Lance dataset path and `--indices`:

```bash
uv run drop_indexes datasets/hits.lance --indices UserID EventDate
uv run drop_indexes datasets/hits.lance --indices UserID_idx EventDate_idx
```

- Positional `dataset` defaults to `datasets/hits.lance`.
- `--indices` accepts index names or indexed column names.
- Omitting `--indices` removes every active index.
- After removal, the tool deletes orphaned UUID directories under `_indices` that
  are no longer referenced by the Lance dataset.

# Query tools

All query commands share the same index, value, field, and value-format behavior.
Each `--indexes` item is paired with the corresponding `--values` item by position,
and conditions are combined with `AND`.

Input formats:

- `UserID`: unsigned 64-bit integer
- `WatchID` and `RefererHash`/`URLHash`: unsigned 64-bit integer
- `EventDate`: `YYYY-MM-DD`
- `ClientIP`: dotted IPv4 address
- Other columns: integer, floating-point, Boolean, or string inferred from the value

The human-readable inputs are converted to the signed or encoded representations
stored in Lance and Parquet. For example, `15985305027620249815` becomes
`-2461439046089301801` for an Int64 `UserID` column.

## Polars query

```bash
uv run query_polars \
  --indexes UserID,EventDate \
  --values "15985305027620249815,2013-07-15" \
  --fields WatchID,EventDate
```

The Polars command supports:

- `--lance [DATASET]` to query a Lance dataset; defaults to `hits`.
- `--parquet [DATASET]` to query a Parquet dataset; defaults to `hits`.
- `--indexes INDEXES` for comma-separated condition columns.
- `--values VALUES` for comma-separated values in the same order as `--indexes`.
- `--fields FIELDS` for comma-separated returned columns, or `*` for every column.

By default, it queries Lance using `hits` and `UserID = -2461439046089301801`:

```bash
uv run query_polars
```

Choose one dataset explicitly:

```bash
uv run query_polars --lance hits
uv run query_polars --parquet hits
```

If both `--lance` and `--parquet` are specified, Polars queries and compares both
result sets and reports if their rows differ.

## DuckDB query

```bash
uv run query_duckdb \
  --indexes UserID,EventDate \
  --values "15985305027620249815,2013-07-15" \
  --fields WatchID,EventDate
```

The DuckDB command uses the same arguments and dataset selection behavior as
`query_polars`. For Lance, it uses native Lance filtering and DuckDB result
ordering; for Parquet, it reads the dataset directly with DuckDB.

```bash
uv run query_duckdb --lance hits
uv run query_duckdb --parquet hits
uv run query_duckdb \
  --indexes UserID,EventDate \
  --values "15985305027620249815,2013-07-15" \
  --fields WatchID,UserID,EventDate
```

## Native Lance query

```bash
uv run query_native \
  --indexes UserID,EventDate \
  --values "15985305027620249815,2013-07-15" \
  --fields WatchID,EventDate
```

`query_native` accepts `--lance [DATASET]`, `--indexes`, `--values`, and `--fields`,
but it does not support querying Parquet or comparing two engines. It uses Lance
scanners directly with Lance scalar-index filtering and native result ordering,
without importing Polars or DuckDB.

```bash
uv run query_native --lance hits
uv run query_native --indexes UserID --values "15985305027620249815"
```

## Query response

Every query implementation reports:

- The selected dataset path
- Execution time
- Output row count
- Output column count
- Estimated in-memory result size

For Lance queries, the response also includes the generated Lance filter and
`LanceRead`/`ScalarIndexQuery` plan metrics. Use `--fields` to control the projected
result columns; it accepts a comma-separated field list and defaults to `*`, which
returns every column.

# Disk layout

The current Lance dataset occupies approximately **24 GB**. Data files occupy
approximately **23 GB**, while active index directories occupy approximately
**1.07 GB**.

| Component | Approximate size | Share of dataset |
|---|---:|---:|
| Total Lance dataset | 24 GB | 100% |
| Data files | 23 GB | ~96% |
| Active indexes | 1.07 GB | ~4% |

Active BTREE indexes:

| Index | Column | Approximate size |
|---|---|---:|
| `UserID_idx` | `UserID` | 651 MB |
| `EventDate_idx` | `EventDate` | 443 MB |

Only active index UUID directories remain under `_indices` after orphan cleanup.

# Performance

## Lookup performance

| Path | Mechanism | Time |
|---|---|---:|
| DuckDB on Parquet | Parallel scan of the full `WatchID` column (~800 MB) plus one row group | ~1.3 s |
| Polars on Parquet | Same scan with additional processing | ~16 s |
| Polars on Lance | BTREE index lookup | ~0.2 s |
| Native Lance | BTREE index lookup | ~0.03 s |
| DuckDB on Lance (Arrow scan) | BTREE index lookup with filter pushdown | ~0.03 s |

## Projection impact

Projecting 4 columns instead of 105 substantially reduced data fetch and processing:

| Metric | 105 columns | 4 columns | Change |
|---|---:|---:|---:|
| Bytes read | 118.2 KB | 42.2 KB | ~2.8× less |
| IOPS | 92 | 7 | ~13× fewer |
| Compute elapsed | 1.49 ms | 0.68 ms | ~2.2× faster |
| Total elapsed | 2.49 ms | 1.23 ms | ~2× faster |

# Source layout

- `src/lance_tools/` contains Lance-specific utilities:
  - `convert_from_parquet.py`
  - `create_indexes.py`
  - `drop_indexes.py`
- `src/queries/` contains engine-specific lookup implementations:
  - `base.py`
  - `polars.py`
  - `duckdb.py`
  - `native.py`
- `src/constants.py` contains shared dataset paths and column formats.

## FastAPI Lance API

The REST API exposes dataset discovery, Arrow schemas, and indexed Lance lookups.
Install the project dependencies and start it with:

```bash
uv sync
uv run api
```

Open the interactive docs at `http://127.0.0.1:8000/docs`.

At startup, the API discovers every Lance dataset and prewarms all of its indexes
before accepting requests. Startup warm-up status is available from `/health`.
Restarting the API repeats the warm-up because Lance index memory is process-local.

- `GET /health`
- `GET /datasets`
- `GET /datasets/{name}/schema`
- `GET /datasets/{name}/query?indexes=UserID&values=15985305027620249815&fields=WatchID,EventDate&limit=100`

Query by sending a `GET` request to the dataset query path:

```bash
curl -G 'http://127.0.0.1:8000/datasets/hits/query' \
  --data-urlencode 'indexes=UserID,EventDate' \
  --data-urlencode 'values=15985305027620249815,2013-07-15' \
  --data-urlencode 'fields=WatchID,EventDate' \
  --data-urlencode 'limit=100'
```

The query parameters are similar to the CLI tools:

- `indexes`: comma-separated condition columns
- `values`: comma-separated values in the same order as `indexes`
- `fields`: comma-separated returned columns, or `*` for every column
- `limit`: maximum number of rows to return

By default, the response includes every column and up to 1,000 rows.
