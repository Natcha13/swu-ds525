import psycopg2


drop_table_queries = [
    "DROP TABLE IF EXISTS events",
    "DROP TABLE IF EXISTS actors",
    "DROP TABLE IF EXISTS orgs",
    "DROP TABLE IF EXISTS repos",
    "DROP TABLE IF EXISTS staging_events"
]

create_table_queries = [
    """
    CREATE TABLE IF NOT EXISTS events (
        id text,
        type text,
        actor text,
        repo text,
        created_at text
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS actors (
        id int,
        login text,
        url text,
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS orgs (
        id text,
        login text,
        url text
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS repos (
        id int,
        name text,
        url text
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS staging_events (
        id text,
        type text,
        actor_id text,
        actor_login text,
        actor_url text,
        repo_id text,
        repo_name text,
        repo_url text,
        org_id text,
        org_login text,
        org_url text,
        public boolean,
        created_at text
    )
    """
]   

copy_table_queries = [
    """
    COPY staging_events FROM 's3://dw-natcha/github_events_01.json'
    CREDENTIALS 'aws_iam_role=arn:aws:iam::225883386411:role/LabRole'
    JSON 's3://dw-natcha/events_json_path.json'
    REGION 'us-east-1'
    """
]

insert_table_queries = [
    """
        INSERT INTO events (id, type, actor, repo, created_at)
        SELECT DISTINCT id, type, actor_login, repo_name, created_at
        FROM staging_events
        WHERE id NOT IN (SELECT DISTINCT id FROM events)
    """,
    """
        INSERT INTO actors (id, login, url)
        SELECT DISTINCT actor_id, actor_login, actor_url
        FROM staging_events
        WHERE actor_id NOT IN (SELECT DISTINCT id FROM actors)
    """,
    """
        INSERT INTO orgs (id, login, url)
        SELECT DISTINCT org_id, org_login, org_url
        FROM staging_events
        WHERE org_id NOT IN (SELECT DISTINCT id FROM orgs)
    """,
    """
    INSERT INTO repos (id, name, url)
    SELECT DISTINCT repo_id, repo_name, repo_url
    FROM staging_events
    WHERE repo_id NOT IN (SELECT DISTINCT id FROM repos)
    """
]

def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def load_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    host = "redshift-cluster-dw-natcha.csxgrh3xyrof.us-east-1.redshift.amazonaws.com"
    dbname = "dev"
    user = "awsuser"
    password = "DWnatcha13"
    port = "5439"
    conn_str = f"host={host} dbname={dbname} user={user} password={password} port={port}"
    conn = psycopg2.connect(conn_str)
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)
    load_tables(cur, conn)
    insert_tables(cur, conn)

    query = "select * from events"
    cur.execute(query)
    records = cur.fetchall()
    for row in records:
        print(row)

    conn.close()


if __name__ == "__main__":
    main()