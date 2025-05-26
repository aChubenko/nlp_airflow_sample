import time
import psycopg2
from sickle import Sickle
from sickle.oaiexceptions import OAIError
from requests.exceptions import HTTPError

# Подключение к PostgreSQL
PG_CONN = {
    "host": "localhost",
    "port": 5432,
    "dbname": "arxiv",
    "user": "postgres",
    "password": "your_password"
}

# Инициализируем Sickle
sickle = Sickle("http://export.arxiv.org/oai2")

# Обёртка с автоповтором при 503
def safe_list_records(**kwargs):
    while True:
        try:
            return sickle.ListRecords(**kwargs)
        except HTTPError as e:
            if e.response.status_code == 503:
                retry_after = int(e.response.headers.get("Retry-After", "30"))
                print(f"⏳ arXiv ответил 503. Ждём {retry_after} сек...")
                time.sleep(retry_after)
            else:
                raise

# Сохранение батча в PostgreSQL
def save_batch_to_db(batch):
    if not batch:
        return
    with psycopg2.connect(**PG_CONN) as conn:
        with conn.cursor() as cur:
            for rec in batch:
                cur.execute("""
                    INSERT INTO arxiv_articles (identifier, title, abstract, authors, created)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (identifier) DO NOTHING
                """, (
                    rec["id"],
                    rec["title"],
                    rec["abstract"],
                    ", ".join(rec["authors"]),
                    rec["created"]
                ))

def save_batch_to_db2(batch):
    print(f"save_batch_to_db2 {len(batch)}")

# Основной цикл по всем страницам
def harvest_arxiv(year: int):
    total = 0
    buffer = []

    # Начинаем с первой страницы
    records = safe_list_records(
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

            # 💾 Сохраняем каждые 1000 записей
            if len(buffer) >= 1000:
                print(f"📦 Сохраняем {len(buffer)} записей...")
                time.sleep(10)
                save_batch_to_db2(buffer)
                total += len(buffer)
                print(f"✅ Всего сохранено: {total}")
                buffer = []

        # Получаем токен и переходим к следующей странице
        token = records.resumption_token
        if token:
            print(f"token batch size : {len(buffer)}")
            time.sleep(10)
            print(f"➡️ Переходим по resumptionToken: {token}")
            records = safe_list_records(
                metadataPrefix="arXiv",
                resumptionToken=token
            )
        else:
            print("✅ Все страницы пройдены.")
            break

    # Сохраняем остаток
    if buffer:
        print(f"📦 Финальный батч: {len(buffer)} записей")
        save_batch_to_db2(buffer)
        total += len(buffer)

    print(f"🎉 Завершено. Всего сохранено: {total} записей")

if __name__ == "__main__":
    harvest_arxiv(2016)
