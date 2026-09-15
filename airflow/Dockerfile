FROM apache/airflow:3.2.2
ARG AIRFLOW_VERSION=3.2.2

# requirements.txt already pins apache-airflow-core at this version, so pip
# is not asked to resolve the meta-package against it a second time (the
# previous `pip install apache-airflow==${VERSION} -r requirements.txt` made
# pip solve for both, which is how the two drift apart).
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
