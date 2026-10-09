# Volute · SECOP Colombia ETL (Open Procurement)

Incremental ingestion of Colombia's public procurement system (SECOP I + II)
from the official Socrata API (`datos.gov.co`) into Postgres. No HTML scraping.
Idempotent: re-runs never duplicate.

## Sources

| Dataset | Socrata ID | Logical PK | Watermark |
|---|---|---|---|
| SECOP II Procesos (tenders) | `p6dx-8zbt` | `(id_del_proceso, id_adjudicacion, codigoproveedor, valor_llave)` — one row per awardee | `fecha_de_ultima_publicaci` |
| SECOP II Contratos | `jbjy-vk9h` | `(id_contrato)` | `fecha_de_firma` (`ultima_actualizacion` is mostly null) |
| SECOP I Procesos | `f789-7hwg` | `(uid)` | `ultima_actualizacion` |
| SECOP II Docs (download ref, 2025+) | `dmgg-8hin` | `(id_documento)` | `fecha_carga` |

Physical pagination by Socrata `:id` (`ORDER BY wm, :id` + keyset), independent
of the logical PK. Monotonic watermark + 2-day overlap (upsert dedups).

## Quickstart (Docker)

```bash
cp .env.example .env   # set SOCRATA_APP_TOKEN + API_KEYS
make init              # postgres + schema
make backfill-open START=2026-01 END=2026-11   # open tenders only
make scheduler         # daily incremental loop (24h)
make psql
```

## Usage

```bash
pip install -r requirements.txt
python -m etl.sync --dataset secop2_procesos --limit 5 --dry-run -v
python -m etl.backfill --dataset secop2_procesos --open-only --start 2026-01
make reset   # wipe data (keeps schema). CAREFUL.
```

Open tenders = everything except `Seleccionado`/`Cancelado`. The daily job runs
*unfiltered* so status transitions are captured; queries filter open states.

## Attachments (for the agent)

`etl/attachments.py` + `secop_documentos`: metadata (Socrata) → binary (public
`RetrieveFile` endpoint, no login) → text (pypdf) → Spanish full-text index.

```bash
make docs-ingest docs-download docs-extract
make docs-search Q="estudios previos hospital"
```

Known limit: open data has no `REQ<->BDOS` bridge and the portal ficha asks for
captcha even to real browsers → no exact "docs of THIS REQ". The agent queries
by entity/text (FTS); pgvector embeddings on top (`make embed-sample`).

## API + Scalar + MCP (read-only, API key)

```bash
docker compose up -d api mcp   # or: make api-up
```

- REST: `/licitaciones` (filters + `next`/`prev` paging), `/licitaciones/{id}`,
  `/licitaciones-semantic`, `/documentos/search`, `/documentos/{id}`,
  `/live/licitaciones`, `/live/documentos` (live Socrata: full history)
- Docs: `localhost:8000/scalar`
- MCP: `:8001/sse` + STDIO (`make mcp-stdio-test`), tools: search, detail,
  semantic, docs, + live variants
- Auth: `X-API-Key` (or `?api_key=`); `/health`, `/scalar`, `/openapi.json` open
- Cache: TTL (`CACHE_TTL`, default 600s), `"cached": true/false`, `?fresh=1` bypasses

## Layout

```
etl/config.py       env + datasets + open-states filter
etl/socrata.py      SODA client + (wm, :id) keyset + retries
etl/db.py           schema/watermark/upsert + normalization
etl/sync.py         incremental CLI (overlap anti-loss)
etl/backfill.py     resumable monthly runner (backfill_progress)
etl/attachments.py  docs metadata/binaries/text/FTS
etl/scheduler.py    daily loop | etl/embed.py  pgvector embeddings
api/                FastAPI + Scalar + MCP (queries/live/cache/auth)
```

## Known limits

- Socrata: `LIMIT` max 50000 (we use 1000-5000); `count(*)` times out on huge
  sets → we never use it for ingestion.
- Dirty data (`No Definido`, URL objects) → raw kept in `raw:jsonb`.
- Attachments of ALL history ≈ hundreds of TB; open-only ≈ 10-15 TB — download
  selectively.
