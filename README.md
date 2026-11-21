Overall picture

Path	Mechanism	Time
DuckDB on Parquet	parallel scan of the full WatchID column (~800 MB) plus one row group	~1.3s
Polars on Parquet	the same scan plus more work	~16s
Polars on Lance	BTREE index lookup	~0.2s
DuckDB on Lance (Arrow scan)	BTREE index lookup, filter pushed down	~0.03s


Projecting 4 columns instead of 105 cut the data fetch substantially, and my prediction was close.

Metric	105 columns	4 columns	Change
bytes_read	118.2 K	42.2 K	~2.8x less
iops	92	7	~13x fewer
elapsed_compute	1.49 ms	0.68 ms	~2.2x faster
total elapsed	2.49 ms	1.23 ms	~2x faster
