from xml.etree.ElementTree import Element

from airflow.decorators import task
from datetime import datetime
import requests
import logging
import psycopg2
import xml.etree.ElementTree as ET

logger = logging.getLogger(__name__)

PG_CONN = {
    "dbname": "arxiv",
    "user": "airflow",
    "password": "airflow",
    "host": "postgres",
    "port": 5432,
}

ns = {'oai': 'http://www.openarchives.org/OAI/2.0/',
              'arxiv': 'http://arxiv.org/OAI/arXiv/'}

url_first_year_page_template = "http://export.arxiv.org/oai2?verb=ListRecords&from={year}-01-01&until={year}-12-31&metadataPrefix=arXiv"
url_resumption_token_template = "http://export.arxiv.org/oai2?verb=ListRecords&resumptionToken={resumption_token}"

@task
def scrape_first_year_page_articles_list(year: int):
    url = url_first_year_page_template.format(year = year)
    process_articles_list_scrapping(url)

@task
def scrape_resumption_token_articles_list(resumptionToken: str):
    url = url_resumption_token_template.format(resumptionToken = resumptionToken)
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
    scrape_resumption_token_articles_list(token)

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
