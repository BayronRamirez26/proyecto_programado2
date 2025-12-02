FROM apache/spark:4.1.0-preview4-python3

USER root

# Librería para PostgreSQL (psycopg2) y Pandas
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    gcc \
    libpq-dev \
    && apt-get purge -y gcc libpq-dev && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir psycopg2-binary pandas

WORKDIR /app

COPY spark/analytics_spark.py /app/

# Copia el driver JDBC 
COPY jdbc/postgresql-42.7.3.jar /opt/spark/jars/