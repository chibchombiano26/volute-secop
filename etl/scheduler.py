"""Scheduler diario SECOP: loop simple sin dependencias.

Cada REPEAT_SECONDS corre incremental SIN filtro de estado (trae altas +
cambios de estado, ej. Publicado -> Seleccionado) con overlap de 2 dias.
El upsert deduplica; las consultas filtran abiertas con estado_del_procedimiento.

Env:
  REPEAT_SECONDS (default 86400), PAGE_SIZE, RUN_ONCE=1 (una pasada y salir)
"""
import logging
import os
import time

from . import config
from .db import init_schema
from .socrata import SocrataClient
from .sync import sync_dataset

log = logging.getLogger("scheduler")


def run_once(page_size=2000, overlap=2):
    client = SocrataClient()
    if not client.app_token:
        log.warning("Sin SOCRATA_APP_TOKEN (throttle anonimo)")
    only = [k.strip() for k in os.getenv("SCHED_DATASETS", "").split(",") if k.strip()]
    keys = [k for k in config.DATASETS if not only or k in only]
    for k in keys:
        try:
            extra = config.DOCS_PRE_WHERE if k.startswith("secop2_docs_") else None
            sync_dataset(client, k, page_size=page_size, dry_run=False,
                         overlap_days=overlap, extra_where=extra)
        except Exception:
            log.exception("[%s] fallo daily, sigo con el resto", k)


def main():
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    repeat = int(os.getenv("REPEAT_SECONDS", "86400"))
    page = int(os.getenv("PAGE_SIZE", "2000"))
    init_schema()
    if os.getenv("RUN_ONCE") == "1":
        run_once(page_size=page)
        return
    log.info("scheduler cada %ss", repeat)
    while True:
        run_once(page_size=page)
        log.info("ciclo OK, duermo %ss", repeat)
        time.sleep(repeat)


if __name__ == "__main__":
    main()
