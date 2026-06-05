import io
import pyarrow as pa
import pyarrow.parquet as pq

def records_to_parquet_bytes(records: list[dict]) -> bytes:
    table = pa.Table.from_pylist(records)
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="snappy")
    buffer.seek(0)
    return buffer.read()