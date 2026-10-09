"""CLI ETL incremental SECOP -> Postgres.

Uso:
  python -m etl.sync --dataset secop2_procesos --limit 2000
  python -m etl.sync --dataset all --page-size 2000
  python -m etl.sync --dataset secop1_procesos --since 2026-09-01T00:00:00.000 --dry-run
  python -m etl.sync --init-db
"""
import argparse
import logging
import sys

from . import config
from .db import get_conn, get_cursor, init_schema, set_watermark, upsert_batch
from .socrata import SocrataClient

log = logging.getLogger("secop")


def _rewind_days(iso, days):
    """Resta N dias a un ISO 'YYYY-MM-DDTHH:MM:SS.mmm' (solo parte fecha)."""
    try:
        from datetime import datetime, timedelta
        d = datetime.fromisoformat(iso.replace("Z", "").split(".")[0])
        return (d - timedelta(days=days)).strftime("%Y-%m-%dT00:00:00.000")
    except Exception:
        return iso


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="ETL incremental SECOP (datos.gov.co -> Postgres)")
    p.add_argument("--dataset", default="all",
                   choices=list(config.DATASETS.keys()) + ["all"])
    p.add_argument("--since", default=None, help="ISO timestamp inicial, ej 2026-10-01T00:00:00.000 (override watermark)")
    p.add_argument("--until", default=None, help="ISO timestamp final")
    p.add_argument("--extra-where", default=None, help="Filtro SoQL extra, ej \"modalidad_de_contratacion='Minima Cuantia'\"")
    p.add_argument("--page-size", type=int, default=config.PAGE_SIZE)
    p.add_argument("--limit", type=int, default=None, help="Max filas por dataset en esta corrida")
    p.add_argument("--full", action="store_true", help="Ignorar watermark guardado (backfill desde cero)")
    p.add_argument("--overlap-days", type=int, default=2,
                   help="Re-escanear N dias previos en modo incremental (default 2, upsert deduplica)")
    p.add_argument("--no-overlap", action="store_true", help="Desactivar overlap (para backfill por ventanas)")
    p.add_argument("--init-db", action="store_true", help="Crear tablas y salir")
    p.add_argument("--dry-run", action="store_true", help="No escribe en DB, solo consulta API")
    p.add_argument("-v", "--verbose", action="store_true")
    return p.parse_args(argv)


def sync_dataset(client, dataset_key, since=None, until=None, extra_where=None,
                 page_size=1000, max_rows=None, dry_run=False, since_id=None,
                 overlap_days=0):
    ds = config.DATASETS[dataset_key]
    socrata_id = ds["socrata_id"]
    wm_col = ds["watermark"]

    cursor = since
    cursor_id = since_id
    if cursor is None and not dry_run:
        conn = get_conn()
        try:
            cursor, cursor_id = get_cursor(conn, dataset_key)
            if overlap_days and cursor:
                # Re-escanea los ultimos N dias: el upsert deduplica y asi no se
                # pierden filas del mismo dia si la corrida anterior se interrumpio.
                cursor = _rewind_days(cursor, overlap_days)
                cursor_id = None
        finally:
            conn.close()

    log.info("[%s] start since=%s until=%s page=%d (ds=%s wm=%s)",
             dataset_key, cursor, until, page_size, socrata_id, wm_col)

    total = 0
    batches = 0
    last_seen = cursor
    last_id = cursor_id
    for batch, soql in client.iter_incremental(
        socrata_id, wm_col, since=cursor, until=until,
        extra_where=extra_where, page_size=page_size, max_rows=max_rows,
        since_id=cursor_id,
    ):
        batches += 1
        # cursor fisico = :id de la ultima fila (ORDER BY wm ASC, :id ASC)
        tail = batch[-1]
        last_seen = tail.get(wm_col) or last_seen
        last_id = tail.get(":id") or last_id
        if dry_run:
            total += len(batch)
            log.info("[%s] batch %d: %d filas (dry-run) wm=%s :id=%s",
                     dataset_key, batches, len(batch), last_seen, last_id)
            continue

        conn = get_conn()
        try:
            with conn:
                n = upsert_batch(conn, dataset_key, batch)
                if last_seen:
                    set_watermark(conn, dataset_key, socrata_id, wm_col,
                                  last_seen, rows=n, last_pk=last_id)
            total += n
            log.info("[%s] batch %d: upsert %d filas, watermark=%s", dataset_key, batches, n, last_seen)
        finally:
            conn.close()
        # el iterador ya avanza el cursor compuesto internamente; para la
        # siguiente ventana del mismo proceso actualizamos el punto de partida
        cursor, cursor_id = last_seen, last_id
        if max_rows is not None and total >= max_rows:
            break

    log.info("[%s] done: %d filas en %d batches, last=%s", dataset_key, total, batches, last_seen)
    return {"dataset": dataset_key, "rows": total, "batches": batches, "last": last_seen}


def main(argv=None):
    args = parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    if args.init_db:
        init_schema()
        log.info("schema OK")
        return 0

    # init schema siempre (idempotente) salvo dry-run
    if not args.dry_run:
        init_schema()

    client = SocrataClient()
    keys = list(config.DATASETS.keys()) if args.dataset == "all" else [args.dataset]
    rc = 0
    for k in keys:
        try:
            since = args.since
            overlap = 0 if (args.no_overlap or args.since or args.full) else args.overlap_days
            if args.full:
                since = None
            sync_dataset(client, k, since=since, until=args.until,
                         extra_where=args.extra_where, page_size=args.page_size,
                         max_rows=args.limit, dry_run=args.dry_run,
                         overlap_days=overlap)
        except Exception as e:
            log.exception("[%s] ERROR: %s", k, e)
            rc = 1
    return rc


if __name__ == "__main__":
    sys.exit(main())
