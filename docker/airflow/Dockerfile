# Official image tag suffix sets Python (default 2.8.0 image is 3.8).
FROM apache/airflow:2.8.0-python3.11

# BashOperator Glue tasks call `aws glue ...`; the base image has no AWS CLI. Prefer the
# official v2 bundle over `pip install awscli` to avoid boto3/botocore clashes with Airflow.
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl unzip ca-certificates graphviz \
    && ARCH="$(uname -m)" \
    && case "$ARCH" in \
         x86_64) AWSCLI_ARCH=x86_64 ;; \
         aarch64) AWSCLI_ARCH=aarch64 ;; \
         *) echo "Unsupported arch: $ARCH" && exit 1 ;; \
       esac \
    && curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-${AWSCLI_ARCH}.zip" -o /tmp/awscliv2.zip \
    && unzip -q /tmp/awscliv2.zip -d /tmp \
    && /tmp/aws/install --update \
    && rm -rf /tmp/awscliv2.zip /tmp/aws \
    && apt-get purge -y curl unzip \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Extra deps for DAG + ingestion code (see requirements.txt header — not the repo root file).
COPY requirements.txt /requirements-airflow-extra.txt
RUN pip install --no-cache-dir -r /requirements-airflow-extra.txt
