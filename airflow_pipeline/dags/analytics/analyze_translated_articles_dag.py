from airflow.decorators import dag, task
from datetime import datetime
import boto3
import psycopg2
from typing import List

@task
def get_identifiers_to_process() -> List[str]:
    conn = psycopg2.connect(
        dbname="postgres",
        user="airflow",
        password="airflow",
        host="postgres",
        port="5432"
    )
    cur = conn.cursor()
    cur.execute("""
        SELECT identifier FROM arxiv_articles
        WHERE status = 'translate_success'
        LIMIT 20;
    """)
    identifiers = [r[0] for r in cur.fetchall()]
    cur.close()
    conn.close()
    return identifiers

@task
def analyze_one_article(identifier: str) -> None:
    s3 = boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin',
        region_name='us-east-1'
    )

    try:
        obj = s3.get_object(Bucket='translated-articles', Key=f"{identifier}.txt")
        content = obj['Body'].read().decode('utf-8')
        word_count = len(content.split())

        conn = psycopg2.connect(
            dbname="postgres",
            user="airflow",
            password="airflow",
            host="postgres",
            port="5432"
        )
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO translated_articles_analytics (identifier, word_count)
            VALUES (%s, %s)
            ON CONFLICT (identifier) DO UPDATE
            SET word_count = EXCLUDED.word_count;
        """, (identifier, word_count))
        cur.execute("""
            UPDATE arxiv_articles
            SET status = 'analized_success'
            WHERE identifier = %s;
        """, (identifier,))
        conn.commit()
        cur.close()
        conn.close()
        print(f"[OK] {identifier}: {word_count} words")

    except Exception as e:
        print(f"[ERR] {identifier}: {e}")
        conn = psycopg2.connect(
            dbname="postgres",
            user="airflow",
            password="airflow",
            host="postgres",
            port="5432"
        )
        cur = conn.cursor()
        cur.execute("""
            UPDATE arxiv_articles
            SET status = 'analized_error'
            WHERE identifier = %s;
        """, (identifier,))
        conn.commit()
        cur.close()
        conn.close()

@dag(
    schedule_interval='@hourly',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["arxiv", "analytics"]
)
def analyze_translated_articles_dag():
    ids = get_identifiers_to_process()
    ids.map(analyze_one_article)

dag_instance = analyze_translated_articles_dag()
