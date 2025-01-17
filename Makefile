.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: setup-dev
setup-dev: ## Setup development environment
	python3 -m venv venv
	. venv/bin/activate && pip install .[tests]
	@echo "To activate the virtual environment, run:"
	@echo "source venv/bin/activate"

.PHONY: build-whl
build-whl: setup-dev ## Build installable whl file
	# Delete any previous wheels, so different versions don't conflict
	rm dev/include/*
	cd dev
	python3 -m build --outdir dev/include/

.PHONY: docker-run
docker-run: build-whl ## Runs local Airflow for testing
	@if ! lsof -i :8080 | grep LISTEN > /dev/null; then \
		cd dev && astro dev start; \
	else \
		cd dev && astro dev restart; \
	fi

.PHONY: astro-login
astro-login: # Login to Astro cloud
	cd dev && astro login cloud.astronomer-stage.io

.PHONY: deploy
deploy: build-whl astro-login ## Runs local Airflow for testing
	cd dev && astro deploy -f

.PHONY: docker-stop
docker-stop: ## Stop Docker container
	cd dev && astro dev stop
