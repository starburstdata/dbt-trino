start-trino:
	docker network create dbt-net || true
	./docker/dbt/build.sh
	./docker/init_trino.bash

dbt-trino-tests: start-trino
	pip install -r dev_requirements.txt
	tox -r  || ./docker/remove_trino.bash
	./docker/run_tests.bash
	./docker/remove_trino.bash

start-starburst:
	docker network create dbt-net || true
	./docker/dbt/build.sh
	./docker/init_starburst.bash

dbt-starburst-tests: start-starburst
	pip install -r dev_requirements.txt
	tox -r || ./docker/remove_starburst.bash
	./docker/run_tests.bash
	./docker/remove_starburst.bash

dev:
	pre-commit install
