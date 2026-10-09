.PHONY: init up down logs sync sync-dry backfill backfill-open backfill-test reset test psql scheduler scheduler-logs docs-ingest docs-download docs-extract docs-search docs-status api-up api-logs api-test mcp-stdio-test embed-sample embed-full

init:
	cp -n .env.example .env || true
	docker compose up -d db
	sleep 3
	docker compose run --rm etl python -m etl.sync --init-db

up:
	docker compose up -d db
	docker compose up etl

down:
	docker compose down

logs:
	docker compose logs -f

# Incremental (usa watermark guardado, idempotente)
sync:
	docker compose run --rm etl python -m etl.sync --dataset all --page-size 2000

sync-dry:
	docker compose run --rm etl python -m etl.sync --dataset secop2_procesos --limit 5 --dry-run -v

# Backfill historico por ventanas mensuales, reanudable (usa backfill_progress).
# Ej: make backfill START=2015-01 END=2016-01  |  make backfill-test (2 ventanas dry-run)
START ?= 2015-01
END ?= 2026-10
backfill:
	docker compose run --rm etl python -m etl.backfill --dataset all --start $(START) --end $(END) --page-size 5000 --sleep 1

backfill-test:
	docker compose run --rm etl python -m etl.backfill --dataset secop2_procesos --start 2015-01 --end 2015-03 --dry-run --limit-windows 2 -v

# Backfill SOLO abiertas (SECOP II procesos con filtro; resto igual)
backfill-open:
	docker compose run --rm etl python -m etl.backfill --dataset secop2_procesos --open-only --start $(START) --end $(END) --page-size 5000 --sleep 1

# Limpieza total de datos (mantiene schema). CUIDADO: borra todo.
reset:
	docker compose exec db psql -U secop -d secop -c "TRUNCATE secop2_procesos, secop2_contratos, secop1_procesos, ingest_state, backfill_progress;"

# Scheduler diario (loop cada 24h). Ver logs con scheduler-logs.
scheduler:
	docker compose --profile daily up -d scheduler

scheduler-logs:
	docker compose logs -f scheduler

# API + Scalar + MCP
api-up:
	docker compose up -d api mcp
api-logs:
	docker compose logs -f api mcp
api-test:
	curl -s "localhost:8000/licitaciones?estado=Publicado&limit=1" | head -c 300; echo
mcp-stdio-test:
	printf '%s\n%s\n%s\n' '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"t","version":"1"}}}' '{"jsonrpc":"2.0","method":"notifications/initialized"}' '{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}' | docker compose run --rm api python -m api.mcp_server 2>/dev/null | tail -1 | head -c 400; echo

# Embeddings (pgvector). Lento en CPU (~11/s): muestra con 2000, fondo con mas.
embed-sample:
	docker compose run --rm embed python -m etl.embed --limit 2000 --batch 64
embed-full:
	docker compose run --rm embed python -m etl.embed --limit 600000 --batch 64
docs-ingest:
	docker compose run --rm etl python -m etl.attachments ingest --dataset secop2_docs_2025 --limit 5000
docs-download:
	docker compose run --rm etl python -m etl.attachments download --limit 50 --ext pdf --max-mb 30
docs-extract:
	docker compose run --rm etl python -m etl.attachments extract --limit 50
docs-search:
	docker compose run --rm etl python -m etl.attachments search "$(Q)" --limit 10
docs-status:
	docker compose run --rm etl python -m etl.attachments status

test:
	python3 -m unittest discover -s tests -v

psql:
	docker compose exec db psql -U secop -d secop -c "SELECT dataset_key,last_value,rows_synced,updated_at FROM ingest_state; SELECT count(*) FROM secop2_procesos;"
