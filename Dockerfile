FROM apache/airflow:3.2.0

USER root

# ==================================================
# System dependencies
# ==================================================
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    tar \
    bash \
    openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# ==================================================
# Spark Client (spark-submit only)
# ==================================================
ENV SPARK_VERSION=3.5.3
ENV SPARK_HOME=/opt/spark

RUN curl -L https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    | tar -xz -C /opt/ && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark

ENV PATH="${SPARK_HOME}/bin:${PATH}"

# ==================================================
# Back to Airflow user
# ==================================================
USER airflow

# ==================================================
# dependencies
# ==================================================
RUN pip install --no-cache-dir \
    apache-airflow-providers-celery \
    apache-airflow-providers-postgres \
    apache-airflow-providers-amazon \
    apache-airflow-providers-fab \
    "celery[rabbitmq]" \
    "kombu>=5.3" \
    psycopg2-binary \
    pandas \
    pyarrow \
    gunicorn \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.2.0/constraints-3.13.txt"

# ==================================================
# Extra requirements opcionales (sin cambios)
# ==================================================
ARG EXTRA_REQUIREMENTS=""
RUN if [ -n "$EXTRA_REQUIREMENTS" ]; then \
      pip install --no-cache-dir $EXTRA_REQUIREMENTS; \
    fi

# ==================================================
# Workdir
# ==================================================
WORKDIR /opt/airflow
