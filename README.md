# 📚 arXiv NLP Pipeline

This is a modular, production-grade data pipeline for processing scientific papers from arXiv using Apache Airflow.

## 🧩 Features

- ✅ Harvest article metadata via OAI-PMH
- 📥 Download raw PDFs into MinIO
- 🧼 Extract and clean PDF text
- 🌐 Translate articles to Ukrainian using OpenAI GPT-4
- 📊 Analyze and count words
- 🗃 Store analytics in PostgreSQL

## 🛠 Stack

- **Apache Airflow** – orchestration
- **PostgreSQL** – metadata and analytics
- **MinIO** – S3-compatible storage
- **OpenAI API** – translation engine
- **Docker Compose** – dev environment

## 🚀 DAGs

- `harvest_arxiv_multiple_years`: harvest arXiv metadata by year
- `harvest_20_raw_articles_to_s3`: download PDFs for selected articles
- `translate_articles_to_ukrainian_dag`: translate articles via GPT
- `analyze_translated_articles_dag`: compute word counts and store in DB

## 📦 Usage

```bash
docker-compose up airflow-init
docker-compose up
```

Access services:

- Airflow: http://localhost:8080
- MinIO: http://localhost:9001
- PostgreSQL: localhost:5432

## ✅ TODO

- Field classification / topic modeling
- Embedding-based similarity search
- Named entity extraction
- Model evaluation pipeline

---

Built with ❤️ for academic automation.