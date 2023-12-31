# query-time-prediction

Uses EvaDB to help predict execution time of SQL queries. Trains the model based off feature embeddings of query plans stored in a PostgreSQL table.

Pgbench was used to populate a PostgreSQL table with 200 million tuples from `script.sql` and stored the resulting information in `benchmark.csv`.

`insert_queries.py` is used to parse `benchmark.csv` and output query, query plan, query execution time, and feature embeddings to `queries.csv`.

`query_time_predictor.py` trains an XGBoost model on the generated feature embeddings.
