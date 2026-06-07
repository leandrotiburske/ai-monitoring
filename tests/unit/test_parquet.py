import io

import pytest

pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

from utils.parquet import records_to_parquet_bytes


def test_records_to_parquet_bytes_returns_readable_parquet():
    records = [
        {
            "model_id": "gpt-4o",
            "tfii": 62.3,
            "rank": 12,
            "provider": "OpenAI",
        }
    ]

    parquet_data = records_to_parquet_bytes(records)
    table = pq.read_table(io.BytesIO(parquet_data))

    assert isinstance(parquet_data, bytes)
    assert table.num_rows == 1
    assert set(table.column_names) == {"model_id", "tfii", "rank", "provider"}
    assert table.to_pylist()[0]["model_id"] == "gpt-4o"
