import pytest
import boto3
import pyarrow.parquet as pq
import io

from moto import mock_aws
from utils.s3 import upload_to_s3, retrieve_from_s3, upload_parquet_to_s3
from utils.parquet import records_to_parquet_bytes
from utils.transformation import transform_news

BUCKET = "test-bucket"
REGION = "eu-north-1"

@pytest.fixture
def s3_bucket():
    with mock_aws():
        boto3.client("s3", region_name=REGION).create_bucket(
            Bucket=BUCKET,
            CreateBucketConfiguration={"LocationConstraint": REGION}
        )
        yield

def test_ingest_and_transform_news(s3_bucket):
    # Simulate the raw news data that would be ingested
    raw = {"articles": [{"id": "abc",
                         "title": "Test",
                         "url": "https://example.com",
                         "source": "S",
                         "sourceDomain": "example.com",
                         "snippet": "...",
                         "categories": ["General AI"],
                         "publishedAt": "2026-06-04T12:00:00Z",
                         "fetchedAt": "2026-06-04T12:00:00Z"}]}

    # Ingestion task
    raw_key = "raw/news/2026-06-07/news.json"
    upload_to_s3(BUCKET, raw_key, raw)

    # Transformation task
    retrieved = retrieve_from_s3(BUCKET, raw_key)
    transformed = transform_news(retrieved)
    parquet_key = "silver/news/2026-06-07/news.parquet"
    upload_parquet_to_s3(BUCKET, parquet_key, records_to_parquet_bytes(transformed))

    # Verify the Parquet file was uploaded and contains the expected data
    obj = boto3.client("s3", region_name=REGION).get_object(Bucket=BUCKET, Key=parquet_key)
    table = pq.read_table(io.BytesIO(obj["Body"].read()))
    assert table.num_rows == 1
    assert table.column("news_id")[0].as_py() == "abc"
