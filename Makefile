TESTS_DIR := $(if $(TESTS_DIR),$(TESTS_DIR),"./")
.PHONY: install test

install:
		pip install pytest
		pip install -r requirements.txt
		airflow initdb

test:
		pytest -vv $(TESTS_DIR)