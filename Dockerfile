FROM apache/airflow:2.9.0-python3.11

USER root

# install psycopg2, git
RUN apt-get update \
    # pakage required for psycopg2
    && apt-get -y install libpq-dev gcc gosu git wget
    # && pip install psycopg2

RUN apt-get update && apt-get install -y \
    openjdk-17-jdk

# Verify that Java was installed
RUN java -version

COPY dbt /opt/airflow/dbt
RUN chmod -R 777 /opt/airflow/dbt/datamart/

USER airflow

# Required for airflow 
RUN pip install passlib
# Install pakage python 
COPY ./requirements.txt .
RUN pip install -r requirements.txt

ENV PYTHONPATH=${PYTHONPATH}:/opt/airflow/plugins
