# dags/fetch_articles_identifiers_per_year_to_db.py

from __future__ import annotations
from airflow.decorators import dag
from datetime import datetime, timedelta
from typing import Dict, List
from airflow_pipeline.plugins.harvest_operator import HarvestArxivOperator

def get_pg_conn() -> Dict[str, str]:
    return {
        "dbname": "arxiv",
        "user": "airflow",
        "password": "airflow",
        "host": "postgres",
        "port": 5432,
    }

@dag(
    dag_id="harvest_arxiv_multiple_years",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["arxiv", "harvest"],
)
def harvest_arxiv_dag() -> None:
    # for year in range(1991, 2026):
    for year in range(2012, 2014):
        HarvestArxivOperator(
            task_id=f"harvest_arxiv_{year}",
            year=year,
            pg_conn=get_pg_conn()
        )

dag: DAG = harvest_arxiv_dag()
