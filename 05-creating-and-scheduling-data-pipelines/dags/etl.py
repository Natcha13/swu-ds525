import json
import glob
import os
from typing import List

from airflow import DAG
from airflow.utils import timezone
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def _get_files(filepath: str) -> List[str]:
    """
    Description: This function is responsible for listing the files in a directory
    """

    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, "*.json"))
        for f in files:
            all_files.append(os.path.abspath(f))

    num_files = len(all_files)
    print(f"{num_files} files found in {filepath}")

    return all_files


def _create_tables():
    hook = PostgresHook(postgres_conn_id="my_postgres")
    conn = hook.get_conn()
    cur = conn.cursor()

    table_create_actor = """
        CREATE TABLE IF NOT EXISTS Actor (
            actor_id INTEGER PRIMARY KEY,
            actor_login VARCHAR(50) NOT NULL,
            actor_display_login VARCHAR(50) NOT NULL,
            actor_gravatar_id VARCHAR(50),
            actor_url VARCHAR(100) NOT NULL,
            actor_avatar_url VARCHAR(100) NOT NULL
        );
    """
    table_create_repo = """
        CREATE TABLE IF NOT EXISTS Repo (
            repo_id INTEGER PRIMARY KEY,
            repo_name VARCHAR(100) NOT NULL,
            repo_url VARCHAR(150) NOT NULL
        );
    """

    table_create_payload = """
        CREATE TABLE IF NOT EXISTS Payload (
            payload_push_id INTEGER PRIMARY KEY,
            payload_size INTEGER NOT NULL,
            payload_ref VARCHAR(200) NOT NULL
        );
    """

    table_create_org = """
        CREATE TABLE IF NOT EXISTS Org (
            org_id INTEGER PRIMARY KEY,
            org_login VARCHAR(50) NOT NULL,
            org_gravatar_id VARCHAR(100),
            org_url VARCHAR(255) NOT NULL,
            org_avatar_url VARCHAR(255) NOT NULL
        );
    """

    table_create_event = """
        CREATE TABLE IF NOT EXISTS Event (
            event_id INTEGER PRIMARY KEY,
            event_type VARCHAR(50) NOT NULL,
            event_public BOOLEAN NOT NULL,
            event_created_at TIMESTAMP NOT NULL,
            event_repo_id INTEGER NOT NULL,
            event_actor_id INTEGER NOT NULL,
            event_org_id INTEGER,
            event_payload_push_id INTEGER,
            FOREIGN KEY (event_repo_id)     REFERENCES Repo     (repo_id),
            FOREIGN KEY (event_actor_id)    REFERENCES Actor    (actor_id),
            FOREIGN KEY (event_org_id)      REFERENCES Org      (org_id),
            FOREIGN KEY (event_payload_push_id)  REFERENCES Payload  (payload_push_id)
        );
    """

    create_table_queries = [table_create_actor, table_create_repo, table_create_payload, table_create_org, table_create_event]


def _process(**context):
    hook = PostgresHook(postgres_conn_id="my_postgres")
    conn = hook.get_conn()
    cur = conn.cursor()

    ti = context["ti"]

    # Get list of files from filepath
    all_files = ti.xcom_pull(task_ids="get_files", key="return_value")
    # all_files = get_files(filepath)

    for datafile in all_files:
        with open(datafile, "r", encoding="utf-8") as f:
            data = json.loads(f.read())
            for each in data:            

                # Insert data into actors table
                insert_statement = f"""
                    INSERT INTO actor (
                        actor_id,
                        actor_login,
                        actor_display_login,
                        actor_gravatar_id,
                        actor_url,
                        actor_avatar_url
                    ) VALUES ('{each["actor"]["id"]}', '{each["actor"]["login"]}', '{each["actor"]["display_login"]}', '{each["actor"]["gravatar_id"]}', '{each["actor"]["url"]}', '{each["actor"]["avatar_url"]}')

                    ON CONFLICT (actor_id) DO NOTHING
                """
                # print(insert_statement)
                cur.execute(insert_statement)


                # Insert data into repo table
                try:
                    insert_statement = f"""
                    INSERT INTO repo (
                        repo_id,
                        repo_name,
                        repo_url
                    ) VALUES ('{each["repo"]["id"]}', '{each["repo"]["name"]}', '{each["repo"]["url"]}')

                    ON CONFLICT (repo_id) DO NOTHING
                """
                    # print(insert_statement)
                    cur.execute(insert_statement)###
                except KeyError:
                    pass


                # Insert data into payload table
                insert_statement = f"""
                    INSERT INTO payload (
                        payload_push_id,
                        payload_size,
                        payload_ref
                    ) VALUES ('{each["payload"]["id"]}', '{each["payload"]["size"]}', '{each["payload"]["ref"]}')

                    ON CONFLICT (payload_push_id) DO NOTHING
                """
                # print(insert_statement)
                cur.execute(insert_statement)

                # Insert data into org table
                insert_statement = f"""
                    INSERT INTO org (
                        org_id,
                        org_login,
                        org_gravatar_id,
                        org_url,
                        org_avatar_url
                    ) VALUES ('{each["org"]["id"]}', '{each["org"]["login"]}', '{each["org"]["gravatar_id"]}', '{each["org"]["url"]}' '{each["org"]["avatar_url"]}')

                    ON CONFLICT (org_id) DO NOTHING
                """
                # print(insert_statement)
                cur.execute(insert_statement)
                
                # Insert data into  events table

                try:
                    insert_statement = f"""
                        events_id,
                        event_type,
                        event_public,
                        event_created_at,
                        event_repo_id,
                        event_actor_id,
                        event_org_id,
                        event_payload_push_id
                    ) VALUES ('{each["events"]["id"]}', '{each["events"]["type"]}', '{each["events"]["public"]}','{each["events"]["created_at"]}', '{each["repo"]["id"]}', '{each["actor"]["id"]}','{each["org"]["id"]}','{each["payload"]["id"]}')
                    
                    ON CONFLICT (events_id) DO NOTHING
                """
                    # print(insert_statement)
                    cur.execute(insert_statement)
                except:
                    insert_statement = f"""
                        events_id,
                        event_type,
                        event_public,
                        event_created_at,
                        event_repo_id,
                        event_actor_id,
                        event_org_id,
                        event_payload_push_id
                    ) VALUES ('{each["events"]["id"]}', '{each["events"]["type"]}', '{each["events"]["public"]}','{each["events"]["created_at"]}', '{each["repo"]["id"]}', '{each["actor"]["id"]}','{each["org"]["id"]}','{each["payload"]["id"]}')
                    
                    ON CONFLICT (events_id) DO NOTHING
                """
                    # print(insert_statement)
                    cur.execute(insert_statement)


                conn.commit()


with DAG(
    "etl",
    start_date=timezone.datetime(2022, 11, 1),
    schedule="@daily",
    tags=["lab5"],
    catchup=False,
) as dag:

    get_files = PythonOperator(
        task_id="get_files",
        python_callable=_get_files,
        op_kwargs={
            "filepath": "/opt/airflow/dags/data",
        }
    )

    create_tables = PythonOperator(
        task_id="create_tables",
        python_callable=_create_tables,
    )
    
    process = PythonOperator(
        task_id="process",
        python_callable=_process,
    )

    # [get_files, create_tables] >> process
    get_files >> create_tables >> process