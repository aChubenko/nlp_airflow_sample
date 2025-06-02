from airflow.decorators import dag, task
from datetime import datetime, timedelta
import psycopg2
import boto3
import os
import logging
import time
from io import BytesIO
from PyPDF2 import PdfReader
from openai import OpenAI

logger = logging.getLogger(__name__)

PG_CONN = {
    "dbname": "arxiv",
    "user": "airflow",
    "password": "airflow",
    "host": "postgres",
    "port": 5432,
}

MINIO_ENDPOINT = "http://minio:9000"
MINIO_INPUT_BUCKET = "raw-articles"
MINIO_OUTPUT_BUCKET = "cleaned-articles"
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

@dag(
    dag_id="clean_success_articles_to_s3_2",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    is_paused_upon_creation=True,
    tags=["draft", "experimental"]
)
def clean_success_articles_to_s3_2():

    @task()
    def get_successful_articles(**context):
        self = context['ti']
        self.log.info("📥 Fetching up to 20 successful articles from PostgreSQL...")
        with psycopg2.connect(**PG_CONN) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT identifier FROM arxiv_articles
                    WHERE status = 'download_success'
                    LIMIT 20;
                """)
                rows = cur.fetchall()
                self.log.info(f"🔎 Retrieved {len(rows)} articles to clean.")
                return [r[0] for r in rows]

    @task()
    def translate_and_save(identifiers: list[str], **context):
        self = context['ti']
        s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY
        )

        for identifier in identifiers:
            try:
                self.log.info(f"⬇️ Downloading PDF for {identifier}")
                response = s3.get_object(Bucket=MINIO_INPUT_BUCKET, Key=f"{identifier}.pdf")
                pdf_bytes = response['Body'].read()

                reader = PdfReader(BytesIO(pdf_bytes))
                full_text = "\n".join([page.extract_text() or "" for page in reader.pages]).strip()

                if not full_text:
                    raise ValueError("PDF contains no extractable text")

                self.log.info(f"🌐 Translating {identifier} via OpenAI")
                response = client.chat.completions.create(
                    model="gpt-4-turbo",
                    messages=[
                        {"role": "system",
                         "content": "Переклади на українську мову науковий текст нижче. Зберігай стиль та терміни."},
                        {"role": "user", "content": full_text[:8000]}
                    ],
                    temperature=0.3
                )
                translated_text = response.choices[0].message.content.strip()

                s3.put_object(
                    Bucket=MINIO_OUTPUT_BUCKET,
                    Key=f"{identifier}.txt",
                    Body=translated_text.encode("utf-8")
                )
                self.log.info(f"✅ Uploaded {identifier}.txt to translated bucket")

                # ✅ Обновляем статус на translate_success
                with psycopg2.connect(**PG_CONN) as conn:
                    with conn.cursor() as cur:
                        cur.execute("""
                            UPDATE arxiv_articles
                            SET status = 'translate_success'
                            WHERE identifier = %s;
                        """, (identifier,))
                        conn.commit()
                        self.log.info(f"🟢 Status updated: translate_success for {identifier}")

            except Exception as e:
                self.log.error(f"❌ Failed to process {identifier}: {e}")
                try:
                    with psycopg2.connect(**PG_CONN) as conn:
                        with conn.cursor() as cur:
                            cur.execute("""
                                UPDATE arxiv_articles
                                SET status = 'translate_error'
                                WHERE identifier = %s;
                            """, (identifier,))
                            conn.commit()
                            self.log.info(f"🔴 Status updated: translate_error for {identifier}")
                except Exception as db_err:
                    self.log.error(f"⚠️ DB status update failed for {identifier}: {db_err}")

            self.log.info("⏳ Waiting 60 seconds before next...")
            time.sleep(60)

    identifiers = get_successful_articles()
    clean_and_upload_sequential(identifiers)

dag = clean_success_articles_to_s3_2()
