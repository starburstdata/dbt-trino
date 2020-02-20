FROM alpine:latest

ENV DBT_VERSION "0.15.2"
ENV DBT_DIR /opt/dbt-presto

RUN apk add python3 python3-dev postgresql-dev build-base libffi-dev git bash
RUN pip3 install --upgrade pip
RUN pip3 install dbt==${DBT_VERSION}
RUN mkdir ${DBT_DIR} ${DBT_DIR}/dbt
ADD dbt ${DBT_DIR}/dbt
ADD setup.py ${DBT_DIR}
ADD README.md ${DBT_DIR}
WORKDIR ${DBT_DIR}
RUN python3 setup.py install