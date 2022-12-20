import json
import glob
import os
from typing import List
from airflow import DAG
from airflow.utils import timezone
from airflow.operators.python import PythonOperator
import boto3
import psycopg2

host = "redshift-cluster-1.csxgrh3xyrof.us-east-1.redshift.amazonaws.com"
dbname = "dev"
user = "awsuser"
password = "Beenan1110"
port = "5439"
conn_str = f"host={host} dbname={dbname} user={user} password={password} port={port}"
conn = psycopg2.connect(conn_str)
cur = conn.cursor()

def _create_tables():

    table_drop_movies_platforms = "DROP TABLE IF EXISTS moviesplatforms"

    table_create_movies_platforms = """ 
        CREATE TABLE IF NOT EXISTS moviesplatforms (
            movie_id int,
            title text,
            year int,
            age_rate text,
            tomatoes_rate text,
            netflix int,
            hulu int,
            prime_video int,
            disney_plus int
        )
    """

    create_table_queries = [
        table_drop_movies_platforms,
        table_create_movies_platforms,
    ]

    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def _load_tables():
    copy_table = """
    COPY moviesplatforms FROM 's3://natcha-capstone/moviesonstreamingplatforms.csv'
    ACCESS_KEY_ID 'ASIATJF5S6IVXTTAPCKO'
    SECRET_ACCESS_KEY 'iydBRRjcVK1fku/8qV/OR8A33i5lnGlq2Q34fEiq'
    SESSION_TOKEN 'FwoGZXIvYXdzEEoaDJm2ZJHoYDraT5au9SLNARm6p7eDAZEuUGVaYWoAoPxqLMHxx4oiPkEO8a5zkGdrLVz20uc6AQ8zNbEzEwfnmViPBFN9L5Y7zIIgTHYGWsVNyZLNfWhLcff7LPit8L9mMYdGse8rq4cPy/dyzXzUi+V8EXC1OczHaTFrt8jf2AUYWfs5UgAwLOb3OnPlubAbM7OYTIW85cxubLrQQhLqf57vSyKJ6Z1soGmoFarKeYJVrugSOlTcbv5JEI1/neqigImDi11xqy3NktT7xpNCOF8Wjxol4k6wDi/cos0osPSFnQYyLbU4noQ50HeHlnGPqAP/k1ewBE7QxoFdRjZ+W6xBA62IYx+76MCJRI/zQZoniA=='
    CSV
    DELIMITER ',' 
    IGNOREHEADER 1 
    """
    
    copy_table_queries = [
        copy_table
    ]

    for cop in copy_table_queries:
        cur.execute(cop)
        conn.commit()


with DAG(
    "etl",
    start_date=timezone.datetime(2022, 12, 20),
    schedule="@daily",
    tags=["workshop"],
    catchup=False,
) as dag:

    create_tables = PythonOperator(
        task_id = "create_tables",
        python_callable = _create_tables,
    )
    
    load_tables = PythonOperator(
        task_id = "load_tables",
        python_callable = _load_tables,
    )


    # [get_files, create_tables] >> process
    create_tables >> load_tables

