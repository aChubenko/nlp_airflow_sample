from airflow_pipeline.tasks.obtain_anual_articles_amount_task import scrape_first_year_page_articles_list
from airflow.decorators import dag
from datetime import datetime


@dag(
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
)
def obtain_annual_articles_amount():
    second_page_tokens = scrape_first_year_page_articles_list.expand(year=list(range(1991, 2025)))

    @task.branch
    def should_continue(next_token: str) -> str:
        if next_token == "END":
            return "stop"
        else:
            return "trigger_next"


    TriggerDagRunOperator(
        task_id='trigger_other_dag',
        trigger_dag_id='target_dag_id',  # ID DAG-а, который ты хочешь вызвать
        conf={"param1": "value1", "param2": 123},  # параметры (опционально)
        wait_for_completion=False,  # True, если хочешь ждать выполнения
    )



obtain_annual_articles = obtain_annual_articles_amount()