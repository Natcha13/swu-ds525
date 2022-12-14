import glob
import json
import os
from typing import List

import psycopg2


def get_files(filepath: str) -> List[str]:
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


def process(cur, conn, filepath):
    # Get list of files from filepath
    all_files = get_files(filepath)

    for datafile in all_files:
        with open(datafile, "r") as f:
            data = json.loads(f.read())
            for each in data:
                # Print some sample data
                
                if each["type"] == "IssueCommentEvent":
                    print(
                        each["id"], 
                        each["type"],
                        each["actor"]["id"],
                        each["actor"]["login"],
                        each["repo"]["id"],
                        each["repo"]["name"],
                        each["created_at"],
                        
                    )
                else:
                    print(
                        each["id"], 
                        each["type"],
                        each["actor"]["id"],
                        each["actor"]["login"],
                        each["repo"]["id"],
                        each["repo"]["name"],
                        each["created_at"],
                    )

                # Insert data into tables here
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

                insert_statement = f"""
                    INSERT INTO repo (
                        repo_id,
                        repo_name,
                        repo_url
                    ) VALUES ('{each["repo"]["id"]}', '{each["repo"]["name"]}', '{each["repo"]["url"]}')

                    ON CONFLICT (repo_id) DO NOTHING
                """
                cur.execute(insert_statement)

            try:
                insert_statement = f"""
                    INSERT INTO payload (
                        payload_push_id,
                        payload_size,
                        payload_ref
                    ) VALUES ('{each["payload"]["id"]}', '{each["payload"]["size"]}', '{each["payload"]["ref"]}')

                    ON CONFLICT (payload_push_id) DO NOTHING
                """
                cur.execute(insert_statement)
            except:
                pass
                
            try:    
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
                cur.execute(insert_statement)
            except:
                pass    

            try:
                insert_statement = f"""
                    INSERT INTO events (
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
                cur.execute(insert_statement)
            except:
                pass    

            conn.commit()
              
def main():
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=postgres user=postgres password=postgres"
    )
    cur = conn.cursor()

    process(cur, conn, filepath="../data")

    conn.close()


if __name__ == "__main__":
    main()