from airflow.decorators import dag, task
from datetime import datetime

@dag(schedule_interval=None, start_date=datetime(2024, 1, 1), catchup=False)
def scrape_all():

    @task
    def scrape(page_id: int):
        print(f"Скрапим страницу {page_id}")

    scrape.expand(page_id=list(range(1000)))

scrape_all_dag = scrape_all()
