import csv
import numpy as np
import psycopg2
from credentials import user, password, api_key
from gpt4all import Embed4All
from openai import OpenAI
from psycopg2.extras import execute_values
from sentence_transformers import SentenceTransformer
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler

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
    client = OpenAI(
        api_key = api_key,
    )
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
    embeddings = []
    # costs = []
    # duration = []
    id = 0
    for i in range(len(queries)):      
        if durations[i] >= 30000:
            try:
                cursor.execute("explain " + queries[i])
                plan = cursor.fetchall()
                plan = ','.join([s[0] for s in plan])
                # indices = []
                # start = 0
                # while True:
                #     index = plan.find("cost", start, len(plan))
                #     if (index == -1):
                #         break
                #     indices.append(index)
                #     start = index + 4
                # cost = 0
                # for index in indices:
                #     start = plan.find("..", index, len(plan)) + 2
                #     end = plan.find(" ", index, len(plan))
                #     cost += float(plan[start:end])
                
                # duration.append(durations[i])
                # costs.append(cost)

                # bert_model = SentenceTransformer('bert-base-nli-mean-tokens')
                # embeddings.append(np.concatenate((bert_model.encode(plan), bert_model.encode(queries[i]))))
                # embeddings = bert_model.encode(plan).tolist() + bert_model.encode(queries[i]).tolist()
                # embeddings = embedder.embed(plan) + embedder.embed(queries[i])
                response_plan = client.embeddings.create(
                    model = "text-embedding-ada-002",
                    input=[plan]
                )
                # response_query = client.embeddings.create(
                #     model = "text-embedding-ada-002",
                #     input=[queries[i]]
                # )
                # embeddings.append(response_plan.data[0].embedding + response_query.data[0].embedding)
                embeddings.append(response_plan.data[0].embedding)
                data = [id, queries[i], plan, durations[i]]
                id += 1

                tuples.append(data)
                
            except Exception as e:
                print(f"Error for statement {i}: {e}")
                conn.rollback()
                continue
    
    scaler = StandardScaler()
    embeddings = scaler.fit_transform(embeddings)
    pca = PCA(n_components=0.9, svd_solver="full")
    embeddings = pca.fit_transform(embeddings)
    
    for i in range(len(tuples)):
        tuples[i] = tuple(tuples[i] + embeddings[i].tolist())
    
    return tuples, len(embeddings[0])

    # return tuples, len(embeddings)

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
    print(num_embeddings)

    create_table(conn, cursor, num_embeddings)
    insert_queries(tuples, num_embeddings)

    # duration, costs = generate_tuples()

    # r = np.corrcoef(np.array(duration), np.array(costs))

    # print(r)