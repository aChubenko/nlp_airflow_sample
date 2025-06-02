from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import requests
import xml.etree.ElementTree as ET

PG_CONN = {
    "dbname": "arxiv",
    "user": "airflow_pipeline",
    "password": "airflow_pipeline",
    "host": "postgres",
    "port": 5432,
}



def obtain_amount_of_publications_per_year():
    publications_years = range(1991, 2025, 1)
    for year in  range(1991, 2025, 1):
        url = f"http://export.arxiv.org/oai2?verb=ListRecords&from={year}-01-01&until={year}-12-31&metadataPrefix=arXiv"

def schedule_dag_for_year(year):
    url = f"http://export.arxiv.org/oai2?verb=ListRecords&from={year}-01-01&until={year}-12-31&metadataPrefix=arXiv"
    try:
        resp = requests.get(url, timeout=30)
        root = ET.fromstring(resp.content)

        token_elem = root.find(".//{http://www.openarchives.org/OAI/2.0/}resumptionToken")
        if token_elem is None:
            raise Exception("No resumptionToken found")

        token = token_elem.text.strip()
        cursor = int(token_elem.attrib.get("cursor", 0))
        total = int(token_elem.attrib.get("completeListSize", 0))

        cur.execute("""
            UPDATE arxiv_years
            SET complete_list_size = %s,
                resumption_token = %s,
                cursor = %s,
                status = 'indexed',
                updated_at = now()
            WHERE year = %s
        """, (total, token, cursor, year))
        print(f"{year}: {total} publications, offset={token}")
        conn.commit()

    except Exception as e:
        cur.execute("""
            UPDATE arxiv_years
            SET status = 'error',
                updated_at = now()
            WHERE year = %s
        """, (year,))
        print(f"Error of processing year {year}: {e}")
        conn.commit()

def process_next_year():
    with psycopg2.connect(**PG_CONN) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT year FROM arxiv_years
                WHERE status = 'pending'
                ORDER BY year
                LIMIT 1
            """)
            row = cur.fetchone()
            if not row:
                print("All years are processed")
                return

            year = row[0]
            url = f"http://export.arxiv.org/oai2?verb=ListRecords&from={year}-01-01&until={year}-12-31&metadataPrefix=arXiv"
            print(f"Request for {year}: {url}")
            try:
                resp = requests.get(url, timeout=30)
                root = ET.fromstring(resp.content)

                token_elem = root.find(".//{http://www.openarchives.org/OAI/2.0/}resumptionToken")
                if token_elem is None:
                    raise Exception("No resumptionToken found")

                token = token_elem.text.strip()
                cursor = int(token_elem.attrib.get("cursor", 0))
                total = int(token_elem.attrib.get("completeListSize", 0))

                cur.execute("""
                    UPDATE arxiv_years
                    SET complete_list_size = %s,
                        resumption_token = %s,
                        cursor = %s,
                        status = 'indexed',
                        updated_at = now()
                    WHERE year = %s
                """, (total, token, cursor, year))
                print(f"{year}: {total} publications, offset={token}")
                conn.commit()

            except Exception as e:
                cur.execute("""
                    UPDATE arxiv_years
                    SET status = 'error',
                        updated_at = now()
                    WHERE year = %s
                """, (year,))
                print(f"Error of processing year {year}: {e}")
                conn.commit()

# DAG
default_args = {
    "owner": "airflow_pipeline",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="arxiv_oai_index_years",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="* * * * *",
    catchup=False,
    tags=["arxiv", "index", "years"]
) as dag:

    index_next_year = PythonOperator(
        task_id="index_year_via_oai",
        python_callable=process_next_year
    )
