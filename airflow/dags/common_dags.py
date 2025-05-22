from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

with DAG(
    dag_id="process_arxiv_papers",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    extract_pdfs = PythonOperator(
        task_id="extract_pdfs",
        python_callable=download_from_minio,
    )

    parse_texts = PythonOperator(
        task_id="parse_texts",
        python_callable=extract_text_from_pdfs,
    )

    clean_and_tokenize = PythonOperator(
        task_id="clean_and_tokenize",
        python_callable=clean_texts,
    )

    compute_embeddings = PythonOperator(
        task_id="compute_embeddings",
        python_callable=generate_tfidf_embeddings,  # или позже — scibert_embeddings
    )

    classify_field = PythonOperator(
        task_id="classify_field",
        python_callable=predict_research_field,
    )

    save_to_db = PythonOperator(
        task_id="save_to_db",
        python_callable=save_results_to_database,
    )

    generate_report = PythonOperator(
        task_id="generate_report",
        python_callable=generate_analytics_report,
    )

    (
        extract_pdfs >> parse_texts >> clean_and_tokenize >>
        compute_embeddings >> classify_field >> save_to_db >> generate_report
    )
