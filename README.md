# Run

The shared dataset configuration is in `src/constants.py`:

- `DATASET_NAME` selects the default dataset name (`hits`).
- `INDEX_COLUMNS` lists the columns indexed by default (`UserID`, `EventDate`, `ClientIP`, `RegionID`).
- `get_parquet_dataset_path(name)` returns `datasets/<name>.parquet`.
- `get_lance_dataset_path(name)` returns `datasets/<name>.lance`.

Convert a Parquet dataset to Lance:

```bash
uv run convert
```

Create BTREE indexes on a Lance dataset:

```bash
uv run indexes
```

By default, this uses `DATASET_NAME` and `INDEX_COLUMNS`. The dataset and
columns can be overridden with positional and named arguments:

```bash
uv run indexes --columns WatchID EventTime
```

Compare Lance and Parquet lookups using comma-separated column names and one
human-readable scalar value for each column. Conditions are combined with `AND`:

```bash
uv run query_polars \
  --indexes UserID,EventDate,ClientIP,RegionID \
  --values "15985305027620249815,2013-07-15,183.122.198.174,839" \
  --fields WatchID,EventDate,ClientIP,RegionID
```

Input formats:

- `UserID`: unsigned 64-bit integer
- `EventDate`: `YYYY-MM-DD`
- `ClientIP`: dotted IPv4 address
- `RegionID`: integer value

The number of items in `--values` must match the number of columns. Values are
paired with columns by position.

The script converts these human-readable values to the signed or encoded values
stored in the dataset before applying equality filters.

By default, query Lance using the `hits` dataset and `UserID = -2461439046089301801`:

```bash
uv run query_polars
```

Explicitly choose a Lance or Parquet dataset:

```bash
uv run query_polars --lance hits
uv run query_polars --parquet hits
```

For custom scalar columns and values:

```bash
uv run query_polars \
  --indexes UserID,EventDate \
  --values "15985305027620249815,2013-07-15" \
  --fields WatchID,UserID,EventDate

uv run query_polars \
  --values "15985305027620249815"

uv run query_polars \
  --parquet hits \
  --values "15985305027620249815"
```

Compare Lance and Parquet lookups with DuckDB using the same arguments:

```bash
uv run query_duckdb \
  --indexes UserID,EventDate,ClientIP,RegionID \
  --values "15985305027620249815,2013-07-15,183.122.198.174,839" \
  --fields WatchID,EventDate,ClientIP,RegionID
```

Explicitly choose the Lance or Parquet dataset:

```bash
uv run query_duckdb --lance hits
uv run query_duckdb --parquet hits
```

For custom scalar columns and values:

```bash
uv run query_duckdb \
  --indexes UserID,EventDate \
  --values "15985305027620249815,2013-07-15" \
  --fields WatchID,UserID,EventDate

uv run query_duckdb \
  --parquet hits \
  --values "15985305027620249815"
```

Each query response includes the dataset path, execution time, row and column counts,
estimated result memory size.

Use `--fields` to control returned columns. It accepts a comma-separated field
list and defaults to `*`, which returns every column.

# Disk layout

The current Lance dataset occupies approximately **28 GB**. Data files occupy
approximately **23 GB**, while index directories account for the remaining storage.

| Component | Approximate size |
|---|---:|
| Total Lance dataset | 28 GB |
| Data files | 23 GB |
| All `_indices` directories | ~2.0 GB |
| Active indexes | ~2.05 GB |

Active BTREE indexes:

| Index | Column | Approximate size |
|---|---|---:|
| `UserID_idx` | `UserID` | 651 MB |
| `EventDate_idx` | `EventDate` | 443 MB |
| `ClientIP_idx` | `ClientIP` | 503 MB |
| `RegionID_idx` | `RegionID` | 449 MB |

Only active index UUID directories remain under `_indices` after orphan cleanup.

# Performance

## Lookup performance

| Path | Mechanism | Time |
|---|---|---:|
| DuckDB on Parquet | Parallel scan of the full `WatchID` column (~800 MB) plus one row group | ~1.3 s |
| Polars on Parquet | Same scan with additional processing | ~16 s |
| Polars on Lance | BTREE index lookup | ~0.2 s |
| DuckDB on Lance (Arrow scan) | BTREE index lookup with filter pushdown | ~0.03 s |

## Projection impact

Projecting 4 columns instead of 105 substantially reduced data fetch and processing:

| Metric | 105 columns | 4 columns | Change |
|---|---:|---:|---:|
| Bytes read | 118.2 KB | 42.2 KB | ~2.8× less |
| IOPS | 92 | 7 | ~13× fewer |
| Compute elapsed | 1.49 ms | 0.68 ms | ~2.2× faster |
| Total elapsed | 2.49 ms | 1.23 ms | ~2× faster |

Remove Lance indexes:

```bash
# Remove every existing index
uv run drop_indexes

# Remove selected indexes by column name or index name
uv run drop_indexes datasets/hits.lance --indices UserID EventDate
uv run drop_indexes datasets/hits.lance --indices UserID_idx EventDate_idx
```

Removing active indexes also deletes orphan `_indices` UUID directories that are no
longer referenced by the Lance dataset.
