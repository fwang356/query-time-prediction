import evadb 
from credentials import user, password
from sklearn.metrics import r2_score

cursor = evadb.connect().cursor()

params = {
    "user": user,
    "password": password,
    "host": "127.0.0.1",
    "port": "5432",
    "database": "benchmark",
}


# query = f"CREATE DATABASE postgres_data WITH ENGINE = 'postgres', PARAMETERS = {params};"
# cursor.query(query).df()

query = "embeddings1"

for i in range(2, 6):
  query = query + ", embeddings" + str(i)

cursor.query("""
  CREATE OR REPLACE FUNCTION PredictQueryTime FROM
( SELECT duration, %s FROM postgres_data.queries LIMIT 300)
  TYPE XGBoost
  PREDICT 'duration'
  TASK 'regression'
  TIME_LIMIT 300;
""" %query).df()

predictions = cursor.query("""
  SELECT duration, predicted_duration FROM postgres_data.queries
  JOIN LATERAL PredictQueryTime(duration, %s) AS Predicted(predicted_duration)
  ORDER BY id DESC LIMIT 150;
""" %query).df()

print("R2 Score: " + str(r2_score(predictions["duration"], predictions["predicted_duration"])))
