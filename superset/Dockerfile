# superset-docker/Dockerfile
FROM apache/superset:latest

USER root

RUN pip install duckdb-engine

COPY superset_config.py /app/pythonpath/superset_config.py

USER superset
