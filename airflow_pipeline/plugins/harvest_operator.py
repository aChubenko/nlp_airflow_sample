# airflow_pipeline/plugins/harvest_operator.py
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import psycopg2
import time
from sickle import Sickle
from requests.exceptions import HTTPError
from psycopg2.extras import execute_values
from sickle.oaiexceptions import NoRecordsMatch
from psycopg2 import DatabaseError

class HarvestArxivOperator(BaseOperator):
    @apply_defaults
    def __init__(self, year: int, pg_conn: dict, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.year = year
        self.pg_conn = pg_conn
        self.sickle = Sickle("http://export.arxiv.org/oai2")

    def safe_list_records(self, **kwargs):
        retries = 0
        max_retries = 5

        while retries < max_retries:
            try:
                return self.sickle.ListRecords(**kwargs)
            except NoRecordsMatch:
                self.log.warning("🟡 Нет записей за указанный период — пропускаем.")
                return None
            except HTTPError as e:
                if e.response.status_code == 503:
                    retry_after = int(e.response.headers.get("Retry-After", "30"))
                    self.log.warning(f"503 received. Retrying in {retry_after} seconds...")
                    time.sleep(retry_after)
                else:
                    raise
            except ConnectionError as e:
                self.log.warning(f"🔌 Connection error: {e}. Retrying in 10 sec...")
                time.sleep(100)
                retries += 1
        return None

    # todo move to separate task
    def save_batch_to_db(self, batch):
        if not batch:
            self.log.info("save_batch_to_db empty batch")
            return
        self.log.info(f"save_batch_to_db, {len(batch)}")

        insert_query = """
            INSERT INTO arxiv_articles (
                identifier,
                title,
                summary,
                published,
                authors,
                updated,
                raw_category,
                pdf_url,
                status
            )
            VALUES %s
            ON CONFLICT (identifier) DO UPDATE SET
                title = EXCLUDED.title,
                summary = EXCLUDED.summary,
                published = EXCLUDED.published,
                authors = EXCLUDED.authors,
                updated = EXCLUDED.updated,
                raw_category = EXCLUDED.raw_category,
                pdf_url = EXCLUDED.pdf_url
        """

        #todo to mark articles that are taken to process it is required to create an aditional table with id and statuses
        # ,
        # status = EXCLUDED.status

        values = [
            (
                rec["identifier"],  # oai:arXiv.org:...
                rec["title"],  # <title>
                rec["abstract"],  # <abstract>
                rec["created"],  # <created> (published)
                rec.get("authors") or [],  # authors: list → TEXT[]
                rec.get("datestamp", ""),  # <datestamp> (updated)
                rec.get("categories", ""),  # <categories>
                rec.get("pdf_url", f"https://arxiv.org/pdf/{rec['id']}.pdf"),  # fallback
                "pending"
            )
            for rec in batch
        ]

        try:
            with psycopg2.connect(**self.pg_conn) as conn:
                with conn.cursor() as cur:
                    execute_values(cur, insert_query, values)
                    self.log.info(f"✅ save_batch_to_db: {cur.rowcount}")
        except DatabaseError as e:
            self.log.error(f"❌ save_batch_to_db: insert error — {e}")
            raise

    def execute(self, context):
        year = self.year
        total = 0
        buffer = []

        records = self.safe_list_records(
            metadataPrefix="arXiv",
            **{
                "from": f"{year}-01-01",
                "until": f"{year}-12-31"
            }
        )

        while True:
            for record in records:
                if record.deleted:
                    continue
                meta = record.metadata
                buffer.append({
                    "identifier": record.header.identifier,
                    "id": meta.get("id", [""])[0],
                    "title": meta.get("title", [""])[0],
                    "abstract": meta.get("abstract", [""])[0],
                    "authors": meta.get("creator", []),
                    "created": meta.get("created", [""])[0],
                    "datestamp": record.header.datestamp,  # из <header>
                    "categories": meta.get("categories", [""])[0],
                    "pdf_url": f"https://arxiv.org/pdf/{meta.get('id', [''])[0]}.pdf"
                })
                if len(buffer) >= 1000:
                    self.log.info(f"📦 Saving 1000 records...")
                    self.save_batch_to_db(buffer)
                    total += len(buffer)
                    buffer = []

            token = records.resumption_token
            if token:
                self.log.info(f"➡️ Transition with resumptionToken: {token}")
                time.sleep(10)
                records = self.safe_list_records(
                    metadataPrefix="arXiv",
                    resumptionToken=token
                )
            else:
                self.log.info("✅ All pages processed")
                break

        if buffer:
            self.log.info(f"📦 Финальный батч: {len(buffer)} записей")
            self.save_batch_to_db(buffer)
            total += len(buffer)

        self.log.info(f"🎉 Всего записей сохранено: {total}")
        return total
