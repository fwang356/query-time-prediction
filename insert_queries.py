import csv
import psycopg2
from credentials import user, password
from gpt4all import Embed4All
from psycopg2.extras import execute_values

def create_table(conn, cursor, num_embeddings):
    query = ""

    for i in range(1, num_embeddings + 1):
        query = query + ", embeddings" + str(i) + " float"

    command = """
    CREATE TABLE queries (
                id int,
                query varchar,
                plan varchar,
                duration float %s
                );
                """ %query
    
    cursor.execute(command)
    conn.commit()
    

def generate_tuples():
    embedder = Embed4All()
    with open ("benchmark.csv", mode="r") as file:
        csvFile = csv.DictReader(file)

        queries = []
        durations = []

        for line in csvFile:
            val = line["ending log output to stderr"]
            log_type = val.split(" ")[0]
            if log_type == "statement:":
                queries.append(val[11:])
            elif log_type == "duration:":
                durations.append(float(val.split(" ")[1]))

    
    tuples = []
    id = 0
    for i in range(len(queries)):
        if durations[i] >= 30000:
            try:
                cursor.execute("explain " + queries[i])
                plan = cursor.fetchall()
                plan = ','.join([s[0] for s in plan])
                embeddings = embedder.embed(plan) + embedder.embed(queries[i])
                data = tuple([id, queries[i], plan, durations[i]] + embeddings)
                id += 1

                tuples.append(data)
                
            except Exception as e:
                print(f"Error for statement {i}: {e}")
                conn.rollback()
                continue
    
    return tuples, len(embeddings)

# Parse benchmark csv created by pgbench and inserts query statement, plan, and execution time to the queries csv.
def insert_queries(tuples, num_embeddings):
    try:
        query = ""
        for i in range(1, num_embeddings + 1):
            query += ", embeddings" + str(i)
        execute_values(cursor, "INSERT INTO queries (id, query, plan, duration" + query + ") VALUES %s", tuples)
        conn.commit()
    except Exception as e:
        print(f"Error for statement {i}: {e}")
        conn.rollback()

if __name__ == "__main__":
    conn = psycopg2.connect(database="benchmark", user=user, password=password, host='127.0.0.1', port='5432')
    conn.autocommit = True
    cursor = conn.cursor()
    
    tuples, num_embeddings = generate_tuples()

    create_table(conn, cursor, num_embeddings)
    insert_queries(tuples, num_embeddings)