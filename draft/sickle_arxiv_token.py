from sickle import Sickle

# arXiv OAI-PMH endpoint
sickle = Sickle("http://export.arxiv.org/oai2")

# Аргументы из твоей урлы
records = sickle.ListRecords(
    metadataPrefix='arXiv',
    **{
        'from': '2016-01-01',
        'until': '2016-12-31'
    }
)
print("🔍 Первая запись:")
buffer = []
total = 0

def save_batch_to_db(batch):
    print(f"save_batch_to_db {len(buffer)}")

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

    # ⏱ Сохраняем каждые 1000 записей
    if len(buffer) >= 1000:
        save_batch_to_db(buffer)
        total += len(buffer)
        print(f"💾 Сохранили {total} записей...")
        buffer = []

# 🔚 Сохраняем оставшееся, если есть
if buffer:
    save_batch_to_db(buffer)
    total += len(buffer)
    print(f"💾 Сохранили финальные {len(buffer)}. Всего: {total}")



# try:
#     first = next(records)
#     print("🆔", first.header.identifier)
#     print("📄", first.metadata.get("title"))
# except StopIteration:
#     print("❌ Нет записей")
#
# # Проверим токен
# token = records.resumption_token
# if token:
#     print("\n🔁 resumptionToken найден:", token)
# else:
#     print("\n✅ resumptionToken не найден — это последняя страница")
