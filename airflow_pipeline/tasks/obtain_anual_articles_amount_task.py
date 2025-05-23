from xml.etree.ElementTree import Element
from airflow.decorators import task
import requests
import logging
import psycopg2
import xml.etree.ElementTree as ET
from airflow_pipeline.utils.configs import url_first_year_page_template, url_resumption_token_template, PG_CONN, ns

logger = logging.getLogger(__name__)

@task
def scrape_first_year_page_articles_list(year: int):
    url = url_first_year_page_template.format(year = year)
    process_articles_list_scrapping(url)

@task
def scrape_resumption_token_articles_list(resumption_token: str):
    url = url_resumption_token_template.format(resumption_token = resumption_token)
    process_articles_list_scrapping(url)

def process_articles_list_scrapping(url: str):
    root = make_request(url)
    records = parse_elements(root)
    if records is not None:
        save_records_to_db(records)
        move_to_next_page(parse_resumption_token(root))


def parse_resumption_token(root: Element):
    return root.find(".//oai:resumptionToken", ns).text.strip()

def move_to_next_page(token: str):
    conf = dag_run.conf or {}
    new_conf = conf.copy()
    new_conf["page_number"] = int(conf.get("page_number", 0)) + 1
    new_conf["token"] = token
    trigger_goodbye = TriggerDagRunOperator(
        task_id="trigger_goodbye",
        trigger_dag_id="goodbye_dag",
        conf=new_conf,
        wait_for_completion=False,  # пусть работает независимо
    )

def make_request(url: str):
    try:
        resp = requests.get(url, timeout=30)
        root = ET.fromstring(resp.content)
        return root
    except Exception as e:
        logger.error(f"Fail to make request {url}")
        return None

def parse_elements(root: Element):
    if root is not None:
        records = root.findall(".//oai:record", ns)
        logger.info(f"Found records: {len(records)}")
        return records
    else:
        return None

def save_records_to_db(records: list[Element]):
    with psycopg2.connect(**PG_CONN) as conn:
        with conn.cursor() as cur:
            for rec in records:
                header = rec.find("oai:header", ns)
                if header is None or header.attrib.get("status") == "deleted":
                    continue

                identifier = header.find("oai:identifier", ns).text

                meta = rec.find("oai:metadata", ns)
                if meta is None:
                    continue

                arxiv_data = meta.find("arxiv:arXiv", ns)
                title = arxiv_data.find("arxiv:title", ns).text.strip()
                summary = arxiv_data.find("arxiv:abstract", ns).text.strip()
                published = arxiv_data.find("arxiv:created", ns).text.strip()
                updated_str = arxiv_data.find("arxiv:updated", ns).text.strip()
                category = arxiv_data.find("arxiv:categories", ns).text.strip()
                authors = [e.find("arxiv:keyname", ns).text for e in
                           arxiv_data.findall("arxiv:authors/arxiv:author", ns)]

                arxiv_id = identifier.split(":")[-1]
                pdf_url = f"https://arxiv.org/pdf/{arxiv_id}.pdf"

                cur.execute("""
                    INSERT INTO arxiv_articles (identifier, title, summary, published, authors, year, raw_category, pdf_url, status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'pending')
                    ON CONFLICT (identifier) DO NOTHING
                """, (identifier, title, summary, published, authors, updated_str, category, pdf_url))
