import time

import lance
import pyarrow as pa
import pyarrow.parquet as pq

pa.set_cpu_count(2)
pa.set_io_thread_count(2)

SRC = "datasets/hits.parquet"
DST = "datasets/hits.lance"

pf = pq.ParquetFile(SRC, pre_buffer=False, memory_map=False)
schema = pf.schema_arrow
total = pf.metadata.num_rows


def batches():
    done = 0
    for i, b in enumerate(pf.iter_batches(batch_size=32_768, use_threads=False)):
        done += b.num_rows
        if i % 100 == 0:
            print(f"{done:,}/{total:,} ({done / total:.1%})", flush=True)
            time.sleep(0.5)  # pausa para que baje la temperatura
        yield b


reader = pa.RecordBatchReader.from_batches(schema, batches())

hits = lance.write_dataset(
    reader,
    DST,
    schema=schema,
    mode="create",
    max_rows_per_file=2_000_000,
    max_rows_per_group=32_768,
)

PK = "WatchID"

hits.create_scalar_index(PK, "BTREE")
