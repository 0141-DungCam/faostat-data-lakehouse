FROM apache/airflow:2.9.0-python3.11

USER root

RUN apt-get update && apt-get install -y openjdk-17-jdk \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \ 
    apache-airflow-providers-apache-spark==4.5.0 \
    pyspark==3.5.0 \
    pandas==2.1.4 \
    # Tương tác với minio
    boto3==1.34.10 \
    # Requests để lấy đc data từ faostat 
    requests==2.31.0 \
    # Tương tác với thư viện catalog được host ở postgres
    psycopg2-binary==2.9.9

