from airflow.dags.obtain_anual_articles_amount_task import scrape_resumption_token_articles_list
from airflow.decorators import dag

@dag
def obtain_page_articles_list(token: str, year: int, page_number: int, completeListSize: int):
    conf = dag_run.conf or {}
    scrape_resumption_token_articles_list(token)