from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import requests
import time
from io import BytesIO
import boto3
from botocore.exceptions import ClientError

# Конфигурация
PG_CONN = {
    "dbname": "arxiv",
    "user": "airflow",
    "password": "airflow",
    "host": "postgres",
    "port": 5432,
}

AWS_ACCESS_KEY = "your_access_key"
AWS_SECRET_KEY = "your_secret_key"
AWS_REGION = "eu-central-1"
BUCKET = "raw-pdf-articles"

s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION
)

def ensure_bucket():
    try:
        s3.head_bucket(Bucket=BUCKET)
    except ClientError:
        s3.create_bucket(Bucket=BUCKET)

def download_and_upload():
    ensure_bucket()

    with psycopg2.connect(**PG_CONN) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, pdf_url FROM arxiv_papers
                WHERE status = 'pending'
                LIMIT 10
                FOR UPDATE SKIP LOCKED;
            """)
            rows = cur.fetchall()

            for arxiv_id, pdf_url in rows:
                print(f"⬇️ Скачиваем {arxiv_id}")
                try:
                    cur.execute("""
                        UPDATE arxiv_papers SET status = 'downloading'
                        WHERE id = %s
                    """, (arxiv_id,))
                    conn.commit()

                    response = requests.get(pdf_url, headers={
                        "User-Agent": "arxiv-downloader/1.0 (your_email@example.com)"
                    }, timeout=30)
                    time.sleep(1.5)

                    if response.status_code == 200:
                        data = BytesIO(response.content)
                        s3.upload_fileobj(
                            Fileobj=data,
                            Bucket=BUCKET,
                            Key=f"{arxiv_id}.pdf",
                            ExtraArgs={"ContentType": "application/pdf"}
                        )
                        cur.execute("""
                            UPDATE arxiv_papers SET status = 'done'
                            WHERE id = %s
                        """, (arxiv_id,))
                        print(f"✅ Загружено в S3: {arxiv_id}.pdf")
                    else:
                        raise Exception(f"HTTP {response.status_code}")

                except Exception as e:
                    print(f"❌ Ошибка: {str(e)}")
                    cur.execute("""
                        UPDATE arxiv_papers SET status = 'error', error_message = %s
                        WHERE id = %s
                    """, (str(e), arxiv_id))

                conn.commit()

# Airflow DAG

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="arxiv_download_pdfs_to_s3",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="*/15 * * * *",
    catchup=False,
    tags=["arxiv", "download", "s3"]
) as dag:

    download_task = PythonOperator(
        task_id="download_and_upload_pdfs",
        python_callable=download_and_upload
    )
