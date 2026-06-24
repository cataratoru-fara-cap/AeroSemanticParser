FROM apache/airflow:3.2.2
ARG AIRFLOW_VERSION=3.2.2
ADD requirements.txt .
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt