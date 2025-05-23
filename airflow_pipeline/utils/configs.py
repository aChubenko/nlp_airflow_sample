url_first_year_page_template = "http://export.arxiv.org/oai2?verb=ListRecords&from={year}-01-01&until={year}-12-31&metadataPrefix=arXiv"
url_resumption_token_template = "http://export.arxiv.org/oai2?verb=ListRecords&resumptionToken={resumption_token}"
pdf_url = "https://arxiv.org/pdf/{arxiv_id}.pdf"


PG_CONN = {
    "dbname": "arxiv",
    "user": "airflow_pipeline",
    "password": "airflow_pipeline",
    "host": "postgres",
    "port": 5432,
}

ns = {'oai': 'http://www.openarchives.org/OAI/2.0/',
              'arxiv': 'http://arxiv.org/OAI/arXiv/'}