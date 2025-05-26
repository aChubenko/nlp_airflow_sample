from airflow.decorators import dag
from datetime import datetime
from airflow_pipeline.tasks.scrape_all_articles_id_per_year_task import scrape_articles_list_per_year


@dag(
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
)
def obtain_annual_articles_amount_2():
    scrape_articles_list_per_year.expand(year=list(range(1991, 2025)))

obtain_annual_articles_2 = obtain_annual_articles_amount_2()