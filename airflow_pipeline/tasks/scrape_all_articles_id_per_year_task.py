from airflow.decorators import task
import logging

from airflow_pipeline.domain.scraping import make_request, parse_elements, save_records_to_db, parse_resumption_token
from airflow_pipeline.shared.accesses import url_first_year_page_template, url_resumption_token_template

logger = logging.getLogger(__name__)

@task
def scrape_articles_list_per_year(year: int):
    counter = 1
    url = url_first_year_page_template.format(year = year)
    logger.info(f"scrape_articles_list_per_year {year}")
    token = obtain_response(url, counter)
    while token is not None:
        counter += 1
        url = url_resumption_token_template.format(resumption_token=token)
        token = obtain_response(url, counter)

def obtain_response(url: str, counter: int):
    logger.info(f"obtain_response, counter : {counter}, url : {url}")
    root = make_request(url)
    records = parse_elements(root)
    if records is not None:
        save_records_to_db(records)
    logger.info(f"obtain_response parsing token {url}")
    return parse_resumption_token(root)