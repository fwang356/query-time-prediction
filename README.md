# query-time-prediction

Uses EvaDB to help predict execution time of SQL queries. Trains the model based off feature embeddings of query plans stored in a PostgreSQL table.

benchmark.csv was created using pgbench in PostgreSQL. insert_queries.py and feature_embeddings.py are used to parse benchmark.csv and outputs queries.csv.

query_time_prediction.py is the converted Jupyter notebook to train the model.
