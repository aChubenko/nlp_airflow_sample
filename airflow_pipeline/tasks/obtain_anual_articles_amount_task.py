from xml.etree.ElementTree import Element
from airflow.decorators import task
import requests
import logging

logger = logging.getLogger(__name__)

# @task
# def scrape_first_year_page_articles_list(year: int):
#     url = url_first_year_page_template.format(year = year)
#     return process_articles_list_scrapping(url)
#
# @task
# def scrape_resumption_token_articles_list(resumption_token: str):
#     url = url_resumption_token_template.format(resumption_token = resumption_token)
#     return process_articles_list_scrapping(url)
