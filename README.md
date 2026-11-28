# Lakehouse architecture with random access

A benchmarking project for evaluating random-access and filtered query performance across table formats, query engines, and object storage.

## Index

1. [Description](#1-description)
2. [Technologies](#2-technologies)
3. [Data architecture](#3-data-architecture)
4. [Lance tools](#4-lance-tools)
5. [Query performance analysis](#5-query-performance-analysis)
6. [Docker setup](#6-docker-setup)

## 1. Description

This repository compares the same `hits` dataset stored in Apache Parquet and Lance. It benchmarks full scans, scalar-index lookups, projection pushdown, and native Lance filtering with DuckDB, Polars, and the Lance API.

The target architecture uses a **table catalog** as the metadata and table-management layer and **SeaweedFS** as the S3-compatible object-storage provider. SeaweedFS stores table data and related catalog artifacts while client tools address datasets through the catalog and S3 APIs.

### 1.1 Source code layout

The source code and configuration are organized as follows:

```text
.
├── README.md
├── pyproject.toml
├── uv.lock
├── docker/
│   ├── Dockerfile
│   └── compose.yaml
├── src/
│   ├── api.py
│   ├── constants.py
│   ├── datasets/
│   ├── lance_tools/
│   ├── models/
│   └── queries/
├── datasets/
├── notebooks/
└── .venv/
```

## 2. Technologies

| Technology | Purpose |
|---|---|
| Python 3.12 | Application and benchmark runtime |
| Lance / pylance | Columnar Lance dataset storage, BTREE indexes, native filtering, and scanning |
| Apache Parquet | Baseline columnar file format and full-scan comparison |
| PyArrow | Arrow tables and interoperability |
| Polars | DataFrame query and Lance/Parquet comparison |
| DuckDB | SQL and parallel scan benchmark engine |
| FastAPI / Uvicorn | HTTP API for datasets and indexed lookups |
| Table catalog | Planned metadata, namespace, table, and snapshot discovery layer |
| SeaweedFS | S3-compatible object-storage provider |
| Docker Compose | Local API and SeaweedFS environment |
| `uv` | Python dependency and command management |

## 3. Data architecture

### 3. Data access

Lance datasets use `UserID`, `EventDate`, `ClientIP`, and `RegionID` BTREE indexes. Parquet remains useful as a portable analytical baseline. The table catalog can expose both formats as catalog-managed tables rather than requiring applications to know local filesystem paths.

```text
Client / API / Benchmark
          |
          v
     Table catalog
          |
          v
        SeaweedFS 
     (Object store)
          |
          v
   Lance or Parquet data
```

The table catalog is intended to resolve table names, storage locations, schemas, and versions. SeaweedFS supplies the S3-compatible object store and stores the physical Lance or Parquet objects. This separation allows benchmark workloads to use catalog-managed table locations without embedding environment-specific paths in query code.

### 3.2. Table formats


| Format | Role in this project | Main access pattern |
|---|---|---|
| Lance | Indexed primary benchmark format | Native Lance scanner, BTREE scalar indexes, and projection pushdown |
| Apache Parquet | Scan and compatibility baseline | Parallel column scans with DuckDB or Polars |


## 4. Lance tools

The shared dataset configuration is in `src/constants.py`:

- `DATASET_NAME` selects the default dataset name (`hits`).
- `INDEX_COLUMNS` lists the configured BTREE index columns (`UserID`, `EventDate`, `ClientIP`, `RegionID`).
- `get_parquet_dataset_path(name)` returns `datasets/<name>.parquet`.
- `get_lance_dataset_path(name)` returns `datasets/<name>.lance`.

## 4.1 Convert Parquet to Lance

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

## 4.2 Create Lance indexes

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

## 4.3 Remove Lance indexes

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

## 5. Query performance analysis

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

## 5.1 Polars query

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

## 5.2 DuckDB query

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

## 5.3 Native Lance query

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

## 5.4 Query response configuration

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

## 5.5 Disk layout

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

## 5.6 Lookup performance

| Path | Mechanism | Time |
|---|---|---:|
| DuckDB on Parquet | Parallel scan of the full `WatchID` column (~800 MB) plus one row group | ~1.3 s |
| Polars on Parquet | Same scan with additional processing | ~16 s |
| Polars on Lance | BTREE index lookup | ~0.2 s |
| Native Lance | BTREE index lookup | ~0.03 s |
| DuckDB on Lance (Arrow scan) | BTREE index lookup with filter pushdown | ~0.03 s |

## 5.7 Projection impact

Projecting 4 columns instead of 105 substantially reduced data fetch and processing:

| Metric | 105 columns | 4 columns | Change |
|---|---:|---:|---:|
| Bytes read | 118.2 KB | 42.2 KB | ~2.8× less |
| IOPS | 92 | 7 | ~13× fewer |
| Compute elapsed | 1.49 ms | 0.68 ms | ~2.2× faster |
| Total elapsed | 2.49 ms | 1.23 ms | ~2× faster |

## 6. Docker setup

Start the complete local environment with:

```bash
docker compose -f docker/compose.yaml up --build
```

The Compose project starts SeaweedFS as the S3-compatible object-store provider and the Lance API as the HTTP query service.

## 6.1 Environment variables

The API and S3 credentials can be supplied in the shell or a `.env` file:

```env
LANCE_API_PREWARM=1
AWS_ACCESS_KEY_ID=admin
AWS_SECRET_ACCESS_KEY=adminadmin
AWS_REGION=us-east-1
```

| Variable | Default | Description |
|---|---:|---|
| `LANCE_API_PREWARM` | `0` | Set to `1`, `true`, or `yes` to preload every active Lance index for every discovered dataset during API startup. |
| `AWS_ACCESS_KEY_ID` | `admin` | Access key used by S3-compatible clients. |
| `AWS_SECRET_ACCESS_KEY` | `adminadmin` | Secret key used by S3-compatible clients. |
| `AWS_REGION` | `us-east-1` | AWS region name exposed to S3-compatible clients. |

Without prewarming, indexes are loaded lazily when first queried. Startup warm-up status is available from `/health`.

## 6.2 S3 server

SeaweedFS provides the S3-compatible object-store endpoint:

- S3 endpoint: `http://127.0.0.1:8333`
- Master: `http://127.0.0.1:9333`
- Filer: `http://127.0.0.1:8888`
- Storage volume: `seaweedfs-data`

Containers access SeaweedFS through `http://seaweedfs:8333`. Dataset files from the host `datasets/` directory are mounted separately at `/app/datasets` for local benchmarking.

## 6.3 API

The Lance API exposes dataset discovery, Arrow schemas, and indexed lookups:

- `GET /health`
- `GET /datasets`
- `GET /datasets/{name}/schema`
- `GET /datasets/{name}/query?indexes=UserID&values=15985305027620249815&fields=WatchID,EventDate&limit=100`

The API is available at `http://127.0.0.1:8000`. Open the interactive documentation at `http://127.0.0.1:8000/docs`.

Start the API without Docker:

```bash
uv sync
uv run api
```

Query a dataset with:

```bash
curl -G 'http://127.0.0.1:8000/datasets/hits/query' \
  --data-urlencode 'indexes=UserID,EventDate' \
  --data-urlencode 'values=15985305027620249815,2013-07-15' \
  --data-urlencode 'fields=WatchID,EventDate' \
  --data-urlencode 'limit=100'
```

The query parameters are:

- `indexes`: comma-separated condition columns
- `values`: comma-separated values in the same order as `indexes`
- `fields`: comma-separated returned columns, or `*` for every column
- `limit`: maximum number of rows to return

By default, the response includes every column and up to 1,000 rows.
