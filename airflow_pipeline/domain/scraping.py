from xml.etree.ElementTree import Element
import logging
import requests
import psycopg2
import xml.etree.ElementTree as ET
from airflow_pipeline.utils.configs import url_first_year_page_template, url_resumption_token_template, PG_CONN, ns

logger = logging.getLogger(__name__)

def process_articles_list_scrapping(url: str):
    print(f"Parce 1 resumption token for url {url}")
    logger.info(f"Parce 1 l resumption token for url {url}")

    root = make_request(url)

    if root is None:
        logger.error(f"❌ Empty root for URL: {url}")
        return None

    records = parse_elements(root)
    if records is not None:
        save_records_to_db(records)
    print(f"Parce resumption token for url {url}")
    logger.info(f"Parce l resumption token for url {url}")
    return parse_resumption_token(root)


def parse_resumption_token(root: Element):

    return root.find(".//oai:resumptionToken", ns).text.strip()


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
