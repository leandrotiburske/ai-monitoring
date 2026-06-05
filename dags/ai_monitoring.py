from datetime import datetime
import logging
import os

import requests
from airflow.sdk import DAG, task
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator

from utils.parquet import records_to_parquet_bytes
from utils.s3 import generate_s3_key, retrieve_from_s3, upload_parquet_to_s3, upload_to_s3
from utils.transformation import transform_news as transform_news_records, transform_models as transform_models_records, transform_providers as transforms_providers_records

NEWS_ENDPOINT = "https://tensorfeed.ai/api/news?limit=10"
MODELS_ENDPOINT = "https://tensorfeed.ai/api/models"
BUCKET_NAME = os.getenv("BUCKET_NAME")

logger = logging.getLogger(__name__)

def _fetch_data(endpoint):
    response = requests.get(endpoint, timeout=30)
    response.raise_for_status()
    return response.json()


with DAG(
    dag_id="ai-monitoring",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    ####################
    # Ingestion Layer
    ####################
    @task(task_id="fetch_news")
    def fetch_news():
        data = _fetch_data(NEWS_ENDPOINT)

        logger.info("Fetched %s articles", len(data))

        # Generate S3 key for news data
        s3_key = generate_s3_key("raw/news", "news.json")

        # Upload data to S3
        upload_to_s3(
            BUCKET_NAME,
            s3_key,
            data)
        
        logger.info("Uploaded news data to S3: %s", s3_key)

        return s3_key

    @task(task_id="fetch_models")
    def fetch_models():
        data = _fetch_data(MODELS_ENDPOINT)

        logger.info("Fetched %s models", len(data))

        # Generate S3 key for models data
        s3_key = generate_s3_key("raw/models", "models.json")

        # Upload data to S3
        upload_to_s3(
            BUCKET_NAME,
            s3_key,
            data)

        logger.info("Uploaded models data to S3: %s", s3_key)

        return s3_key
    
    ####################
    # Transformation Layer
    ####################
    @task(task_id="transform_news")
    def transform_news_task(news_s3_key):
        # Retrieve news data from S3
        news = retrieve_from_s3(BUCKET_NAME, news_s3_key)
        transformed_news = transform_news_records(news)

        # Convert from list[dict] to parquet and upload to S3
        parquet_key = generate_s3_key("transformed/news", "news.parquet")
        parquet_data = records_to_parquet_bytes(transformed_news)
        upload_parquet_to_s3(
            BUCKET_NAME,
            parquet_key,
            parquet_data,
            content_type="application/vnd.apache.parquet")

        logger.info("Uploaded transformed news data to S3: %s", parquet_key)
        return parquet_key
    
    @task(task_id="transform_models")
    def transform_models_task(models_s3_key):
        # Retrieve models data from s3
        models_raw = retrieve_from_s3(BUCKET_NAME, models_s3_key)
        models_transformed = transform_models_records(models_raw)
        providers_transformed = transforms_providers_records(models_raw)

        # Convert from list[dict] to parquet
        models_parquet = records_to_parquet_bytes(models_transformed)
        providers_parquet = records_to_parquet_bytes(providers_transformed)

        # Save parquet to S3
        models_key = generate_s3_key("transformed/models", "models.parquet")
        providers_key = generate_s3_key("transformed/providers", "providers.parquet")

        upload_parquet_to_s3(
            BUCKET_NAME,
            models_key,
            models_parquet)
        
        logger.info("Uploaded transformed models data to S3: %s", models_key)

        upload_parquet_to_s3(
            BUCKET_NAME,
            providers_key,
            providers_parquet)

        logger.info("Uploaded transformed providers data to S3: %s", providers_key)

        return {"models_key": models_key, "providers_key": providers_key}
    
    ####################
    # Trigger Glue Crawler
    ####################

    crawler_name = "ai-monitoring-crawler"
    run_glue_crawler = GlueCrawlerOperator(
        task_id="run_glue_crawler",
        config={"Name": crawler_name},
        aws_conn_id="aws_default",
        wait_for_completion=True,
    )

    ####################
    # Run Athena Query
    ####################

    # Validate Glue Crawler results
    run_athena_query = AthenaOperator(
        task_id="run_athena_query",
        query="""
            SELECT COUNT(*)
            FROM news
        """,
        database="ai-monitoring",
        output_location=f"s3://{BUCKET_NAME}/athena-results/",
        aws_conn_id="aws_default",
    )

    news = fetch_news()
    models = fetch_models()
    transformed_news = transform_news_task(news)
    transformed_models = transform_models_task(models)

    [transformed_news, transformed_models] >> run_glue_crawler
    run_glue_crawler >> run_athena_query
