# airflow_pipeline/plugins/harvest_operator.py

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import psycopg2
import time
from sickle import Sickle
from requests.exceptions import HTTPError
from psycopg2.extras import execute_values

class HarvestArxivOperator(BaseOperator):
    @apply_defaults
    def __init__(self, year: int, pg_conn: dict, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.year = year
        self.pg_conn = pg_conn
        self.sickle = Sickle("http://export.arxiv.org/oai2")

    def safe_list_records(self, **kwargs):
        while True:
            try:
                return self.sickle.ListRecords(**kwargs)
            except HTTPError as e:
                if e.response.status_code == 503:
                    retry_after = int(e.response.headers.get("Retry-After", "30"))
                    self.log.warning(f"503 received. Retrying in {retry_after} seconds...")
                    time.sleep(retry_after)
                else:
                    raise

    def save_batch_to_db(self, batch):
        if not batch:
            return

        insert_query = """
            INSERT INTO arxiv_articles (identifier, title, abstract, authors, created)
            VALUES %s
            ON CONFLICT (identifier) DO UPDATE SET
                title = EXCLUDED.title,
                abstract = EXCLUDED.abstract,
                authors = EXCLUDED.authors,
                created = EXCLUDED.created
        """

        values = [
            (
                rec["id"],
                rec["title"],
                rec["abstract"],
                ", ".join(rec["authors"]),
                rec["created"]
            )
            for rec in batch
        ]

        with psycopg2.connect(**self.pg_conn) as conn:
            with conn.cursor() as cur:
                execute_values(cur, insert_query, values)
                self.log.info(f"🔄 БД обновлена: всего строк затронуто {cur.rowcount}")

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
                    "id": record.header.identifier,
                    "title": meta.get("title", [""])[0],
                    "abstract": meta.get("abstract", [""])[0],
                    "authors": meta.get("creator", []),
                    "created": meta.get("created", [""])[0],
                })
                if len(buffer) >= 1000:
                    self.log.info(f"📦 Сохраняем 1000 записей...")
                    self.save_batch_to_db(buffer)
                    total += len(buffer)
                    buffer = []

            token = records.resumption_token
            if token:
                self.log.info(f"➡️ Переход по resumptionToken: {token}")
                time.sleep(10)
                records = self.safe_list_records(
                    metadataPrefix="arXiv",
                    resumptionToken=token
                )
            else:
                self.log.info("✅ Все страницы обработаны.")
                break

        if buffer:
            self.log.info(f"📦 Финальный батч: {len(buffer)} записей")
            self.save_batch_to_db(buffer)
            total += len(buffer)

        self.log.info(f"🎉 Всего записей сохранено: {total}")
        return total
