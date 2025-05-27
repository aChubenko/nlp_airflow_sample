from airflow.decorators import dag, task
from datetime import datetime, timedelta
import psycopg2
import boto3
import os
import logging
from io import BytesIO
from PyPDF2 import PdfReader
import openai

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

openai.api_key = os.getenv("OPENAI_API_KEY")

@dag(
    dag_id="clean_success_articles_to_s3",
    schedule_interval="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["arxiv", "minio", "nlp"]
)
def clean_success_articles_to_s3():

    @task()
    def get_successful_articles(**context):
        self = context['ti']
        self.log.info("📥 Fetching up to 20 successful articles from PostgreSQL...")
        with psycopg2.connect(**PG_CONN) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT identifier FROM arxiv_articles
                    WHERE status = 'success'
                    LIMIT 20;
                """)
                rows = cur.fetchall()
                self.log.info(f"🔎 Retrieved {len(rows)} articles to clean.")
                return [r[0] for r in rows]

    @task()
    def clean_and_upload(identifier: str, **context):
        self = context['ti']
        self.log.info(f"⬇️ Downloading PDF for cleaning and translation: {identifier}")
        try:
            s3 = boto3.client(
                "s3",
                endpoint_url=MINIO_ENDPOINT,
                aws_access_key_id=MINIO_ACCESS_KEY,
                aws_secret_access_key=MINIO_SECRET_KEY
            )
            response = s3.get_object(Bucket=MINIO_INPUT_BUCKET, Key=f"{identifier}.pdf")
            pdf_bytes = response['Body'].read()

            # Parse PDF to extract text
            reader = PdfReader(BytesIO(pdf_bytes))
            full_text = "\n".join([page.extract_text() or "" for page in reader.pages])
            full_text = full_text.strip()

            if not full_text:
                raise ValueError("PDF contains no extractable text")

            # Translate to Ukrainian using OpenAI
            self.log.info(f"🌐 Sending article {identifier} to OpenAI for translation...")
            response = openai.ChatCompletion.create(
                model="gpt-4",
                messages=[
                    {"role": "system", "content": "Переклади на українську мову науковий текст нижче. Зберігай стиль та терміни."},
                    {"role": "user", "content": full_text[:8000]}  # trim to fit token limits
                ],
                temperature=0.3
            )
            translated_text = response.choices[0].message.content.strip()

            # Upload cleaned and translated text
            s3.put_object(Bucket=MINIO_OUTPUT_BUCKET, Key=f"{identifier}.txt", Body=translated_text.encode("utf-8"))
            self.log.info(f"✅ Uploaded cleaned & translated text for {identifier}")
        except Exception as e:
            self.log.error(f"❌ Failed to process {identifier}: {e}")

    identifiers = get_successful_articles()
    clean_and_upload.expand(identifier=identifiers)

dag = clean_success_articles_to_s3()
