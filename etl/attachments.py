"""Adjuntos SECOP II: descarga de binarios + texto + FTS para el agente.

Los metadatos viven en secop_documentos (ingesta via sync/backfill con los
datasets secop2_docs_*). Esto baja el binario desde el endpoint publico
RetrieveFile (sin login, verificado), verifica tamanno, calcula sha256,
extrae texto de PDFs y actualiza el tsvector en espanol.

Uso:
  python -m etl.attachments ingest --dataset secop2_docs_2025 --since 2026-09-01T00:00:00.000 --limit 5000
  python -m etl.attachments download --limit 50 --ext pdf --max-mb 30
  python -m etl.attachments extract --limit 50
  python -m etl.attachments search "estudios previos hospital"
  python -m etl.attachments status
"""
import argparse
import hashlib
import logging
import os
import time

import requests

from . import config
from .db import get_conn, init_schema
from .sync import sync_dataset
from .socrata import SocrataClient

log = logging.getLogger("attach")

DOCS_DIR = os.getenv("DOCS_DIR", os.path.join(os.getcwd(), "data", "docs"))
MAX_CHARS = 200_000


def ingest(args):
    init_schema()
    client = SocrataClient()
    keys = [k for k in config.DATASETS if k.startswith("secop2_docs_")] \
        if args.dataset == "all-docs" else [args.dataset]
    for k in keys:
        sync_dataset(client, k, since=args.since, until=args.until,
                     extra_where=config.DOCS_PRE_WHERE if args.pre_only else None,
                     page_size=args.page_size, max_rows=args.limit,
                     dry_run=args.dry_run)


def _pick_pending(conn, limit, ext=None, max_bytes=None):
    with conn.cursor() as cur:
        q = ("SELECT id_documento, doc_url, tamanno, extension, nombre_archivo "
             "FROM secop_documentos WHERE estado='pending' AND fail_count < 5 "
             "AND doc_url IS NOT NULL")
        params = []
        if ext:
            q += " AND extension = %s"
            params.append(ext)
        if max_bytes:
            q += " AND (tamanno IS NULL OR tamanno <= %s)"
            params.append(max_bytes)
        q += " ORDER BY fecha_carga DESC LIMIT %s"
        params.append(limit)
        cur.execute(q, params)
        return cur.fetchall()


def download(args):
    init_schema()
    os.makedirs(DOCS_DIR, exist_ok=True)
    s = requests.Session()
    s.headers.update({"User-Agent": "volute-secop-etl/1.0"})
    if config.SOCRATA_APP_TOKEN:
        pass  # RetrieveFile no usa app token
    conn = get_conn()
    try:
        rows = _pick_pending(conn, args.limit, ext=args.ext,
                             max_bytes=(args.max_mb * 1024 * 1024) if args.max_mb else None)
    finally:
        conn.close()
    log.info("%d pendientes elegidos", len(rows))
    ok = fail = 0
    for doc_id, url, tamanno, ext, nombre in rows:
        sub = os.path.join(DOCS_DIR, str(doc_id // 1000))
        os.makedirs(sub, exist_ok=True)
        path = os.path.join(sub, "%s.%s" % (doc_id, ext or "bin"))
        try:
            r = s.get(url, timeout=120)
            r.raise_for_status()
            data = r.content
            if tamanno and abs(len(data) - int(tamanno)) > max(1024, int(tamanno) * 0.05):
                raise RuntimeError("tamanno difiere: %d vs %s" % (len(data), tamanno))
            with open(path, "wb") as f:
                f.write(data)
            sha = hashlib.sha256(data).hexdigest()
            conn = get_conn()
            try:
                with conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "UPDATE secop_documentos SET estado='downloaded', storage_path=%s,"
                            " sha256=%s, downloaded_at=now(), updated_at=now() WHERE id_documento=%s",
                            (path, sha, doc_id),
                        )
            finally:
                conn.close()
            ok += 1
            log.info("ok %s (%d bytes)", doc_id, len(data))
        except Exception as e:
            fail += 1
            log.warning("fail %s: %s", doc_id, str(e)[:150])
            conn = get_conn()
            try:
                with conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "UPDATE secop_documentos SET fail_count=fail_count+1,"
                            " estado=CASE WHEN fail_count+1>=5 THEN 'failed' ELSE 'pending' END,"
                            " updated_at=now() WHERE id_documento=%s",
                            (doc_id,),
                        )
            finally:
                conn.close()
        time.sleep(args.sleep)
    log.info("download done: ok=%d fail=%d", ok, fail)


def extract(args):
    from pypdf import PdfReader
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id_documento, storage_path FROM secop_documentos "
                "WHERE estado='downloaded' AND texto IS NULL AND extension='pdf' "
                "ORDER BY fecha_carga DESC LIMIT %s",
                (args.limit,),
            )
            rows = cur.fetchall()
    finally:
        conn.close()
    log.info("%d pdfs por extraer", len(rows))
    n = 0
    for doc_id, path in rows:
        try:
            if not path or not os.path.exists(path):
                raise RuntimeError("sin archivo: %s" % path)
            reader = PdfReader(path)
            parts = []
            for page in reader.pages[:100]:
                parts.append(page.extract_text() or "")
                if sum(map(len, parts)) > MAX_CHARS:
                    break
            texto = "\n".join(parts)[:MAX_CHARS]
            conn = get_conn()
            try:
                with conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "UPDATE secop_documentos SET texto=%s,"
                            " texto_tsv=to_tsvector('spanish', %s),"
                            " updated_at=now() WHERE id_documento=%s",
                            (texto, "%s %s" % (doc_id, texto[:50000]), doc_id),
                        )
            finally:
                conn.close()
            n += 1
        except Exception as e:
            log.warning("extract fail %s: %s", doc_id, str(e)[:150])
    log.info("extract done: %d", n)


def search(args):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id_documento, entidad, nombre_archivo,"
                " ts_rank(texto_tsv, plainto_tsquery('spanish', %s)) AS rank"
                " FROM secop_documentos"
                " WHERE texto_tsv @@ plainto_tsquery('spanish', %s)"
                " ORDER BY rank DESC LIMIT %s",
                (args.q, args.q, args.limit),
            )
            for doc_id, ent, nom, rank in cur.fetchall():
                print("%.3f | %s | %s | %s" % (rank, doc_id, (ent or "")[:40], (nom or "")[:60]))
    finally:
        conn.close()


def status(args):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT estado, count(*), pg_size_pretty(sum(tamanno)::bigint) "
                        "FROM secop_documentos GROUP BY 1 ORDER BY 2 DESC")
            for est, n, b in cur.fetchall():
                print("%-10s %8d  %s" % (est, n, b))
            cur.execute("SELECT count(*) FROM secop_documentos WHERE texto IS NOT NULL")
            print("con texto:", cur.fetchone()[0])
    finally:
        conn.close()


def main(argv=None):
    p = argparse.ArgumentParser(description="Adjuntos SECOP II")
    sub = p.add_subparsers(dest="cmd", required=True)
    pi = sub.add_parser("ingest")
    pi.add_argument("--dataset", default="secop2_docs_2025")
    pi.add_argument("--since", default=None)
    pi.add_argument("--until", default=None)
    pi.add_argument("--limit", type=int, default=None)
    pi.add_argument("--page-size", type=int, default=2000)
    pi.add_argument("--pre-only", action="store_true", default=True,
                    help="solo pre-contractuales (default si)")
    pi.add_argument("--no-pre-filter", action="store_true")
    pi.add_argument("--dry-run", action="store_true")
    pd = sub.add_parser("download")
    pd.add_argument("--limit", type=int, default=50)
    pd.add_argument("--ext", default="pdf")
    pd.add_argument("--max-mb", type=int, default=30)
    pd.add_argument("--sleep", type=float, default=1.0)
    pe = sub.add_parser("extract")
    pe.add_argument("--limit", type=int, default=50)
    ps = sub.add_parser("search")
    ps.add_argument("q")
    ps.add_argument("--limit", type=int, default=10)
    sub.add_parser("status")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if getattr(args, "no_pre_filter", False):
        args.pre_only = False
    {"ingest": ingest, "download": download, "extract": extract,
     "search": search, "status": status}[args.cmd](args)


if __name__ == "__main__":
    main()
