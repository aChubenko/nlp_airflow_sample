from airflow.decorators import dag, task
from datetime import datetime, timedelta
import psycopg2
import boto3
import os
import time
from io import BytesIO
from PyPDF2 import PdfReader
from openai import OpenAI

PG_CONN = {
    "dbname": "arxiv",
    "user": "airflow",
    "password": "airflow",
    "host": "postgres",
    "port": 5432,
}

MINIO_ENDPOINT = "http://minio:9000"
MINIO_INPUT_BUCKET = "raw-articles"
MINIO_OUTPUT_BUCKET = "translated-articles"
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")

OPENAI_KEY = os.getenv("OPENAI_API_KEY")
client = OpenAI(api_key=OPENAI_KEY)

@dag(
    dag_id="translate_articles_to_ukrainian",
    schedule_interval="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["arxiv", "translation", "openai"]
)
def translate_articles_to_ukrainian_dag():

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
                self.log.info(f"🔎 Retrieved {len(rows)} articles to translate.")
                return [r[0] for r in rows]

    @task(pool="sequential_pool")
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
                        {"role": "system", "content": "Переклади на українську мову науковий текст нижче. Зберігай стиль та терміни."},
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

                with psycopg2.connect(**PG_CONN) as conn:
                    with conn.cursor() as cur:
                        cur.execute("""
                            UPDATE arxiv_articles
                            SET status = 'translate_success'
                            WHERE identifier = %s;
                        """, (identifier,))
                        conn.commit()
                        self.log.info(f"🟢 Status updated to translate_success for {identifier}")


            except Exception as e:
                self.log.error(f"❌ Failed to process {identifier}: {e}")
                with psycopg2.connect(**PG_CONN) as conn:
                    with conn.cursor() as cur:
                        cur.execute("""
                            UPDATE arxiv_articles
                            SET status = 'translate_error'
                            WHERE identifier = %s;
                        """, (identifier,))
                        conn.commit()
                raise e
            self.log.info("⏳ Waiting 60 seconds before next...")
            time.sleep(60)

    ids = get_successful_articles()
    translate_and_save(ids)

dag = translate_articles_to_ukrainian_dag()
