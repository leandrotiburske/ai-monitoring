from datetime import datetime
import logging
import os

from airflow.sdk import DAG, task
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator

from utils.ingestion import fetch_data
from utils.parquet import records_to_parquet_bytes
from utils.s3 import generate_s3_key, retrieve_from_s3, upload_parquet_to_s3, upload_to_s3
from utils.transformation import transform_news as transform_news_records, transform_models as transform_models_records, transform_providers as transforms_providers_records, transform_gpu_records, transform_intelligence_records

NEWS_ENDPOINT = "https://tensorfeed.ai/api/news?limit=10"
MODELS_ENDPOINT = "https://tensorfeed.ai/api/models"
GPU_PRICING_ENDPOINT = "https://tensorfeed.ai/api/gpu/pricing"
INTELLIGENCE_ENDPOINT = "https://tensorfeed.ai/api/intelligence"
BUCKET_NAME = os.getenv("BUCKET_NAME")

logger = logging.getLogger(__name__)

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
        data = fetch_data(NEWS_ENDPOINT)

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
        data = fetch_data(MODELS_ENDPOINT)

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
    
    @task(task_id="fetch_gpu_pricing")
    def fetch_gpu_pricing():
        data = fetch_data(GPU_PRICING_ENDPOINT)

        logger.info("Fetched GPU pricing.")

        # Generate S3 key for gpu pricing data
        s3_key = generate_s3_key("raw/gpu_pricing", "gpu_pricing.json")

        # Upload data to S3
        upload_to_s3(
            BUCKET_NAME,
            s3_key,
            data)

        logger.info("Uploaded GPU pricing data to S3: %s", s3_key)

        return s3_key
    
    @task(task_id="fetch_intelligence")
    def fetch_intelligence():
        data = fetch_data(INTELLIGENCE_ENDPOINT)

        logger.info("Fetched intelligence data.")

        # Generate S3 key for intelligence data
        s3_key = generate_s3_key("raw/intelligence", "intelligence.json")

        # Upload data to S3
        upload_to_s3(
            BUCKET_NAME,
            s3_key,
            data)

        logger.info("Uploaded intelligence data to S3: %s", s3_key)

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
    
    @task(task_id="transform_gpu_pricing")
    def transform_gpu_pricing_task(gpu_pricing_s3_key):
        # Retrieve gpu pricing data from s3
        gpu_pricing_raw = retrieve_from_s3(BUCKET_NAME, gpu_pricing_s3_key)
        gpu_pricing_transformed = transform_gpu_records(gpu_pricing_raw)

        # Convert from list[dict] to parquet
        gpu_pricing_parquet = records_to_parquet_bytes(gpu_pricing_transformed)

        # Save parquet to S3
        gpu_pricing_key = generate_s3_key("transformed/gpu_pricing", "gpu_pricing.parquet")

        upload_parquet_to_s3(
            BUCKET_NAME,
            gpu_pricing_key,
            gpu_pricing_parquet)
        
        logger.info("Uploaded transformed gpu pricing data to S3: %s", gpu_pricing_key)

        return gpu_pricing_key
    
    @task(task_id="transform_intelligence")
    def transform_intelligence_task(intelligence_s3_key):
        # Retrieve intelligence data from s3
        intelligence_raw = retrieve_from_s3(BUCKET_NAME, intelligence_s3_key)
        intelligence_transformed = transform_intelligence_records(intelligence_raw)

        # Convert from list[dict] to parquet
        intelligence_parquet = records_to_parquet_bytes(intelligence_transformed)

        # Save parquet to S3
        intelligence_key = generate_s3_key("transformed/intelligence", "intelligence.parquet")
        upload_parquet_to_s3(
            BUCKET_NAME,
            intelligence_key,
            intelligence_parquet)
        
        logger.info("Uploaded transformed intelligence data to S3: %s", intelligence_key)
    
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

    # Define task dependencies
    news = fetch_news()
    models = fetch_models()
    gpu_pricing = fetch_gpu_pricing()
    intelligence = fetch_intelligence()
    transformed_news = transform_news_task(news)
    transformed_models = transform_models_task(models)
    transformed_gpu_pricing = transform_gpu_pricing_task(gpu_pricing)
    transformed_intelligence = transform_intelligence_task(intelligence)

    [transformed_news, transformed_models, transformed_gpu_pricing, transformed_intelligence] >> run_glue_crawler
    run_glue_crawler >> run_athena_query
