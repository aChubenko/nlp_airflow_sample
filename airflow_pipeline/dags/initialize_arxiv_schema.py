from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2

PG_CONN = {
    "dbname": "arxiv",
    "user": "airflow",
    "password": "airflow",
    "host": "postgres",
    "port": 5432,
}

def create_tables():
    queries = [
        """
        CREATE TABLE IF NOT EXISTS arxiv_papers (
            id TEXT PRIMARY KEY,
            title TEXT,
            summary TEXT,
            published TIMESTAMP,
            category TEXT,
            pdf_url TEXT,
            status TEXT DEFAULT 'pending',
            error_message TEXT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS arxiv_articles (
            identifier TEXT PRIMARY KEY,
            title TEXT,
            summary TEXT,
            published TIMESTAMP,
            authors TEXT[],
            updated TIMESTAMP,
            raw_category TEXT,
            pdf_url TEXT,
            status TEXT DEFAULT 'pending'
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS arxiv_amount_of_pupications_by_year (
            year INT PRIMARY KEY,
            complete_list_size INT,
            resumption_token TEXT,
            cursor INT,
            status TEXT,
            updated_at TIMESTAMP
        );
        """
    ]

    with psycopg2.connect(**PG_CONN) as conn:
        with conn.cursor() as cur:
            for q in queries:
                cur.execute(q)
        conn.commit()
        print("✅ All tables created or already exist.")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="initialize_arxiv_schema",
    default_args=default_args,
    schedule_interval="@once",
    catchup=False,
    tags=["init", "schema"]
) as dag:

    init_task = PythonOperator(
        task_id="create_arxiv_tables",
        python_callable=create_tables
    )
