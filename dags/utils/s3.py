from datetime import datetime
import boto3
import io
import json

def generate_s3_key(prefix: str, filename: str) -> str:
    date_str = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    return f"{prefix}/{date_str}/{filename}"

def upload_to_s3(bucket_name: str, key: str, data: dict):
    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=json.dumps(data, ensure_ascii=False))

def upload_parquet_to_s3(
    bucket_name: str,
    key: str,
    data: bytes,
    content_type: str = "application/vnd.apache.parquet",
):
    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=data,
        ContentType=content_type)

def retrieve_from_s3(bucket_name, s3_key):
    s3 = boto3.client("s3")
    response = s3.get_object(Bucket=bucket_name, Key=s3_key)
    return json.loads(response['Body'].read().decode('utf-8'))
