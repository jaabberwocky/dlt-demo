# data load tool (dlt) Demo
Saw a Reddit thread on the dlt tool and found it fascinating with its ease of use. Decided to try it out and see how it works.

Two implementations of the pipeline are provided:
1. sync_pipeline.py
2. async_pipeline.py - an asynchronous pipeline which is 50% faster than the synchronous pipeline

## Installation
```
poetry install
poetry run python async_pipeline.py
```

## Query data
Run a shell in the poetry environment and query the data using the duckdb connector.
```
poetry run python
>>> import duckdb
>>> conn = duckdb.connect("chess_pipeline.duckdb")
>>> conn.sql("SHOW TABLES")
```

