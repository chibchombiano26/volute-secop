"""Backfill historico SECOP por ventanas mensuales (reanudable).

Recorre [start, end) mes a mes por dataset y deja marca en
backfill_progress. Re-correr solo procesa ventanas pendientes.
El upsert hace idempotente cada ventana.

Uso:
  python -m etl.backfill --dataset secop2_procesos --start 2015-01 --end 2026-10 --dry-run
  python -m etl.backfill --dataset all --start 2020-01 --page-size 5000 --sleep 1
"""
import argparse
import logging
import sys
import time
from datetime import date

from . import config
from .db import get_conn, init_schema, is_window_done, mark_window_done
from .socrata import SocrataClient
from .sync import sync_dataset

log = logging.getLogger("backfill")

DEFAULT_START = {
    "secop2_procesos": "2015-01",
    "secop2_contratos": "2015-01",
    "secop1_procesos": "2011-01",
    "secop2_docs_2025": "2025-01",
}


def month_windows(start_ym, end_ym):
    """Genera (win_start_iso, win_end_iso) mensuales: [start, end)."""
    y, m = map(int, start_ym.split("-"))
    ey, em = map(int, end_ym.split("-"))
    while (y, m) < (ey, em):
        ws = "%04d-%02d-01T00:00:00.000" % (y, m)
        nm, ny = (m % 12) + 1, y + (1 if m == 12 else 0)
        # until en sync es <= ; usamos ultimo dia aproximado via primer dia del mes siguiente - 1ms:
        # mas simple: until = primer dia del mes siguiente (exclusivo) menos 1 segundo -> usamos <= con 23:59
        # Para evitar huecos usamos until = inicio del mes siguiente y en la query <= lo incluye;
        # el solape de 1 dia entre ventanas lo resuelve el upsert.
        we = "%04d-%02d-01T00:00:00.000" % (ny, nm)
        yield ws, we
        y, m = ny, nm


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Backfill SECOP por ventanas mensuales")
    p.add_argument("--dataset", default="all", choices=list(config.DATASETS) + ["all"])
    p.add_argument("--start", default=None, help="YYYY-MM (default segun dataset)")
    p.add_argument("--end", default=date.today().strftime("%Y-%m"),
                   help="YYYY-MM exclusivo (default mes actual)")
    p.add_argument("--page-size", type=int, default=5000)
    p.add_argument("--sleep", type=float, default=1.0, help="pausa seg entre ventanas")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--redo", action="store_true", help="reprocesar ventanas ya marcadas done")
    p.add_argument("--limit-windows", type=int, default=None, help="solo N ventanas (prueba)")
    p.add_argument("--extra-where", default=None, help="filtro SoQL (ej. solo abiertas)")
    p.add_argument("--open-only", action="store_true", help="solo licitaciones abiertas (SECOP II procesos)")
    p.add_argument("--tag", default="", help="marca en backfill_progress para no mezclar filtros")
    p.add_argument("-v", "--verbose", action="store_true")
    return p.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    if not args.dry_run:
        init_schema()
    client = SocrataClient()
    if not client.app_token:
        log.warning("Sin SOCRATA_APP_TOKEN: throttle ~1000 req/hora. Consigue uno en datos.gov.co -> API -> App Tokens.")

    keys = list(config.DATASETS) if args.dataset == "all" else [args.dataset]
    rc = 0
    for k in keys:
        start = args.start or DEFAULT_START.get(k, "2015-01")
        extra = args.extra_where
        tag = args.tag
        if args.open_only and k == "secop2_procesos":
            extra = config.OPEN_WHERE if not extra else "(%s) AND (%s)" % (extra, config.OPEN_WHERE)
            tag = tag or "open-only"
        wins = list(month_windows(start, args.end))
        if args.limit_windows:
            wins = wins[:args.limit_windows]
        log.info("[%s] %d ventanas %s -> %s filtro=%s", k, len(wins), start, args.end, extra or "-")
        for ws, we in wins:
            if not args.dry_run and not args.redo:
                conn = get_conn()
                try:
                    done = is_window_done(conn, k, ws, we, tag)
                finally:
                    conn.close()
                if done:
                    log.info("[%s] skip %s (done)", k, ws[:7])
                    continue
            try:
                # Ventanas cerradas: sin overlap, cursor exacto.
                res = sync_dataset(client, k, since=ws, until=we,
                                   extra_where=extra,
                                   page_size=args.page_size, dry_run=args.dry_run)
                if not args.dry_run:
                    conn = get_conn()
                    try:
                        with conn:
                            mark_window_done(conn, k, ws, we, res["rows"], tag)
                    finally:
                        conn.close()
            except Exception as e:
                log.exception("[%s] ventana %s ERROR: %s", k, ws[:7], e)
                rc = 1
                time.sleep(5)
            time.sleep(args.sleep)
    return rc


if __name__ == "__main__":
    sys.exit(main())
