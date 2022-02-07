dbt-trino-tests:
	docker network create dbt-net || true
	./docker/dbt/build.sh
	./docker/init_trino.bash
	pip install -r dev_requirements.txt
	tox -r  || ./docker/remove_trino.bash
	./docker/run_tests.bash
	./docker/remove_trino.bash

dbt-starburst-tests:
	docker network create dbt-net || true
	./docker/dbt/build.sh
	./docker/init_starburst.bash
	pip install -r dev_requirements.txt
	tox -r || ./docker/remove_starburst.bash
	./docker/run_tests.bash
	./docker/remove_starburst.bash
