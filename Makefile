.PHONY: up down restart logs ps test test-local clean

up:
	docker compose up -d --build

down:
	docker compose down -v

restart:
	docker compose restart

logs:
	docker compose logs -f

ps:
	docker compose ps

test:
	docker compose run --rm tests

test-local:
	pytest -v tests/test_pipeline.py

clean:
	docker compose down -v --remove-orphans
