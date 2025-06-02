from airflow.decorators import dag, task
from datetime import datetime, timedelta
import psycopg2
import pandas as pd
import logging

logger = logging.getLogger(__name__)

PG_CONN = {
    "dbname": "arxiv",
    "user": "airflow",
    "password": "airflow",
    "host": "postgres",
    "port": 5432,
}

@dag(
    dag_id="analytics_translated_articles",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["analytics", "postgres"]
)
def analytics_translated_articles():

    @task()
    def fetch_data():
        conn = psycopg2.connect(**PG_CONN)
        query = """
            SELECT research_field, word_count
            FROM translated_articles_analytics
            WHERE word_count IS NOT NULL
            LIMIT 50;
        """
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df.to_dict(orient="records")

    @task()
    def compute_statistics(records: list[dict]):
        df = pd.DataFrame(records)
        summary = df.groupby("research_field")["word_count"].agg(["count", "mean", "min", "max"]).reset_index()
        logger.info("📊 Statistics:\n" + summary.to_string(index=False))
        return summary.to_dict(orient="records")

    raw = fetch_data()
    compute_statistics(raw)

dag = analytics_translated_articles()
