from datetime import datetime
import boto3
import io
import json

from botocore.exceptions import ClientError, NoCredentialsError

def generate_s3_key(
        prefix: str,
        filename: str
    ) -> str:
    """
    Generate an S3 key with a timestamp.
    
    :param prefix: Prefix for the S3 key (e.g., "raw/models")
    :type prefix: str
    :param filename: Filename for the S3 key
    :type filename: str
    :return: The generated S3 key
    :rtype: str
    """
    date_str = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    return f"{prefix}/{date_str}/{filename}"

def upload_to_s3(
        bucket_name: str,
        key: str,
        data: dict
    ) -> None:
    """
    Upload data to S3.
    
    :param bucket_name: Name of the S3 bucket
    :type bucket_name: str
    :param key: S3 key for the object
    :type key: str
    :param data: Data to upload
    :type data: dict
    """
    try:
        s3 = boto3.client("s3")
        s3.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=json.dumps(data, ensure_ascii=False))
    except NoCredentialsError:
        raise Exception("AWS credentials not found. Please configure your AWS credentials.")
    except ClientError as e:
        raise Exception(f"Failed to upload data to S3: {e}")

def upload_parquet_to_s3(
    bucket_name: str,
    key: str,
    data: bytes,
    content_type: str = "application/vnd.apache.parquet",
    ) -> None:
    """
    Upload Parquet data to S3.
    
    :param bucket_name: Name of the S3 bucket
    :type bucket_name: str
    :param key: S3 key for the object
    :type key: str
    :param data: Parquet data to upload
    :type data: bytes
    :param content_type: Content type for the S3 object
    :type content_type: str
    """
    try:
        s3 = boto3.client("s3")
        s3.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=data,
            ContentType=content_type)
    except NoCredentialsError:
        raise Exception("AWS credentials not found. Please configure your AWS credentials.")
    except ClientError as e:
        raise Exception(f"Failed to upload Parquet data to S3: {e}")
    
def retrieve_from_s3(
        bucket_name: str,
        s3_key: str
    ) -> dict:
    """
    Retrieve data from S3.

    :param bucket_name: Name of the S3 bucket
    :type bucket_name: str
    :param s3_key: S3 key for the object
    :type s3_key: str
    :return: The retrieved data
    :rtype: dict
    """
    try:
        s3 = boto3.client("s3")
        response = s3.get_object(Bucket=bucket_name, Key=s3_key)
        return json.loads(response['Body'].read().decode('utf-8'))
    except NoCredentialsError:
        raise Exception("AWS credentials not found. Please configure your AWS credentials.")
    except ClientError as e:
        raise Exception(f"Failed to retrieve data from S3: {e}")

