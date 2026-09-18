.EXPORT_ALL_VARIABLES:

DBT_TEST_USER_1=user1
DBT_TEST_USER_2=user2
DBT_TEST_USER_3=user3

start-trino:
	docker network create dbt-net || true
	./docker/init_trino.bash

dbt-trino-tests: start-trino
	pip install -e . -r dev_requirements.txt
	tox -r

start-starburst:
	docker network create dbt-net || true
	./docker/init_starburst.bash

dbt-starburst-tests: start-starburst
	pip install -e . -r dev_requirements.txt
	tox -r

# Both read test.env themselves; sourcing it through the shell would mangle
# values containing backslashes or shell metacharacters.
dbt-galaxy-tests:
	@test -f test.env || { echo "Create test.env from test.env.example first"; exit 1; }
	python -m pytest tests/functional --profile starburst_galaxy

galaxy-routing-probe:
	@test -f test.env || { echo "Create test.env from test.env.example first"; exit 1; }
	python scripts/galaxy_test_setup.py probe

start-starburst-routing:
	docker network create dbt-net || true
	./docker/init_starburst_routing.bash

dbt-starburst-routing-tests: start-starburst-routing
	@test -f test.env || { echo "Create test.env from test.env.example first"; exit 1; }
	pip install -e . -r dev_requirements.txt
	python scripts/starburst_portal_test_setup.py register
	python -m pytest tests/functional/adapter/test_query_routing.py --profile starburst_portal

starburst-routing-probe:
	@test -f test.env || { echo "Create test.env from test.env.example first"; exit 1; }
	python scripts/starburst_portal_test_setup.py probe

dev:
	pre-commit install
