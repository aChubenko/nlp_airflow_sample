from airflow_pipeline.tasks.obtain_anual_articles_amount_task import scrape_first_year_page_articles_list
from airflow.decorators import dag
from datetime import datetime


@dag(
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
)
def obtain_annual_articles_amount():
    scrape_first_year_page_articles_list.expand(year=list(range(1991, 2025)))

obtain_annual_articles = obtain_annual_articles_amount()