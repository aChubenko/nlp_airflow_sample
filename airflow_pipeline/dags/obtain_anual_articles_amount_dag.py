from obtain_anual_articles_amount_task import scrape_first_year_page_articles_list
from airflow.decorators import dag

@dag
def obtain_anual_articles_amount():
    for year in range(1991, 2025, 1):
        scrape_first_year_page_articles_list.expand(year = year)