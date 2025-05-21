from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import xml.etree.ElementTree as ET
import psycopg2

# Конфиг
ARXIV_CATEGORY = "cs.CL"
MAX_RESULTS = 100
ARXIV_API_URL = "http://export.arxiv.org/api/query"
PG_CONN = {
    "dbname": "arxiv",
    "user": "airflow",
    "password": "airflow",
    "host": "postgres",
    "port": 5432,
}

def fetch_and_store(**kwargs):
    # 1. Запрос к API
    params = {
        "search_query": f"cat:{ARXIV_CATEGORY}",
        "start": 0,
        "max_results": MAX_RESULTS,
        "sortBy": "submittedDate",
        "sortOrder": "descending",
    }
    headers = {"User-Agent": "arxiv-bot/1.0 (your_email@example.com)"}
    response = requests.get(ARXIV_API_URL, params=params, headers=headers)
    root = ET.fromstring(response.content)
    ns = {"atom": "http://www.w3.org/2005/Atom"}

    entries = []
    for entry in root.findall("atom:entry", ns):
        arxiv_id = entry.find("atom:id", ns).text.split("/")[-1]
        title = entry.find("atom:title", ns).text.strip()
        summary = entry.find("atom:summary", ns).text.strip()
        published = entry.find("atom:published", ns).text
        pdf_url = f"https://arxiv.org/pdf/{arxiv_id}.pdf"

        entries.append((arxiv_id, title, summary, published, ARXIV_CATEGORY, pdf_url))

    # 2. Запись в PostgreSQL
    with psycopg2.connect(**PG_CONN) as conn:
        with conn.cursor() as cur:
            for arxiv_id, title, summary, published, category, pdf_url in entries:
                cur.execute("""
                    INSERT INTO arxiv_papers (id, title, summary, published, category, pdf_url, status)
                    VALUES (%s, %s, %s, %s, %s, %s, 'pending')
                    ON CONFLICT (id) DO NOTHING;
                """, (arxiv_id, title, summary, published, category, pdf_url))
        conn.commit()

# Airflow DAG
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="arxiv_fetch_metadata",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["arxiv", "metadata"]
) as dag:

    fetch_metadata = PythonOperator(
        task_id="fetch_and_store_metadata",
        python_callable=fetch_and_store
    )
