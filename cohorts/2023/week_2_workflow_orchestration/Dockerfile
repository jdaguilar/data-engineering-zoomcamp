# syntax=docker/dockerfile:1

FROM prefecthq/prefect:2.7.10-python3.9 AS prefectzoomcamp

COPY requirements.txt .
RUN pip install -r requirements.txt --trusted-host pypi.python.org --no-cache-dir
COPY flows /opt/prefect/flows
RUN mkdir -p /opt/prefect/data/yellow
RUN mkdir -p /opt/prefect/data/green
