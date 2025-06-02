from airflow.decorators import dag, task
from airflow.models.param import Param
from datetime import datetime, timedelta
import requests
import psycopg2
import boto3
import os
import logging

PG_CONN = {
    "dbname": "arxiv",
    "user": "airflow",
    "password": "airflow",
    "host": "postgres",
    "port": 5432,
}

MINIO_ENDPOINT = "http://minio:9000"
MINIO_BUCKET = "raw-articles"
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")

@dag(
    dag_id="fetch_20_raw_articles_to_s3",
    schedule_interval="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["arxiv", "minio", "etl"]
)
def fetch_20_raw_articles_to_s3():

    @task()
    def get_pending_articles(**context):
        self = context['ti']
        self.log.info("📥 Fetching 20 pending articles from PostgreSQL...")
        with psycopg2.connect(**PG_CONN) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE arxiv_articles
                    SET status = 'processing'
                    WHERE identifier IN (
                        SELECT identifier FROM arxiv_articles
                        WHERE status = 'pending'
                        LIMIT 20
                        FOR UPDATE SKIP LOCKED
                    )
                    RETURNING identifier, pdf_url;
                """)
                rows = cur.fetchall()
                self.log.info(f"🔎 Retrieved {len(rows)} articles to process.")
                return [{"identifier": r[0], "pdf_url": r[1]} for r in rows]

    @task()
    def download_and_upload(record, **context):
        self = context['ti']
        identifier = record["identifier"]
        pdf_url = record["pdf_url"]
        self.log.info(f"⬇️ Downloading PDF for {identifier} from {pdf_url}")
        status = "download_success"
        try:
            resp = requests.get(pdf_url, timeout=30)
            resp.raise_for_status()
            s3 = boto3.client(
                "s3",
                endpoint_url=MINIO_ENDPOINT,
                aws_access_key_id=MINIO_ACCESS_KEY,
                aws_secret_access_key=MINIO_SECRET_KEY
            )
            s3.put_object(Bucket=MINIO_BUCKET, Key=f"{identifier}.pdf", Body=resp.content)
            self.log.info(f"✅ Uploaded {identifier}.pdf to MinIO bucket '{MINIO_BUCKET}'")
        except Exception as e:
            self.log.error(f"❌ Failed to process {identifier}: {e}")
            status = "download_error"
        return (identifier, status)

    @task()
    def update_status(results, **context):
        self = context['ti']
        self.log.info("📝 Updating statuses in PostgreSQL...")
        with psycopg2.connect(**PG_CONN) as conn:
            with conn.cursor() as cur:
                for identifier, status in results:
                    self.log.info(f"📌 Setting status='{status}' for {identifier}")
                    cur.execute(
                        "UPDATE arxiv_articles SET status = %s WHERE identifier = %s",
                        (status, identifier)
                    )

    records = get_pending_articles()
    results = download_and_upload.expand(record=records)
    update_status(results)

dag = fetch_20_raw_articles_to_s3()
