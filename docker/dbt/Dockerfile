FROM python:3.9.9-slim-bullseye
USER root

ENV DBT_DIR /opt/dbt-trino

RUN apt-get update && \
    apt-get dist-upgrade -y && \
    apt-get install -y --no-install-recommends \
    netcat curl git ssh software-properties-common \
    make build-essential ca-certificates libpq-dev && \
    apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/
RUN pip3 install --upgrade pip
RUN mkdir ${DBT_DIR} ${DBT_DIR}/dbt
ADD dbt ${DBT_DIR}/dbt
ADD setup.py ${DBT_DIR}
ADD README.md ${DBT_DIR}
ADD docker/dbt/jaffle_shop /jaffle_shop
ADD docker/dbt/profiles.yml /root/.dbt/profiles.yml
WORKDIR ${DBT_DIR}
RUN pip3 install .

WORKDIR /usr/app

ENTRYPOINT ["/bin/sh", "-c"]
