dbt-trino-tests:
	docker network create dbt-net || true
	./docker/dbt/build.sh
	./docker/init_trino.bash
	pip install -r dev_requirements.txt
	tox || ./docker/remove_trino.bash
	./docker/remove_trino.bash

dbt-starburst-tests:
	docker network create dbt-net || true
	./docker/dbt/build.sh
	./docker/init_starburst.bash
	pip install -r dev_requirements.txt
	tox || ./docker/remove_starburst.bash
	./docker/remove_starburst.bash
