.EXPORT_ALL_VARIABLES:

DBT_TEST_USER_1=user1
DBT_TEST_USER_2=user2
DBT_TEST_USER_3=user3

start-trino:
	docker network create dbt-net || true
	./docker/init_trino.bash

dbt-trino-tests: start-trino
	pip install -r dev_requirements.txt
	tox -r

start-starburst:
	docker network create dbt-net || true
	./docker/init_starburst.bash

dbt-starburst-tests: start-starburst
	pip install -r dev_requirements.txt
	tox -r

dev:
	pre-commit install
