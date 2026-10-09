"""Embeddings de licitaciones (MiniLM multilingue, 384 dims) para busqueda semantica.

Corre en Docker (modelo ~470MB, cache en volumen). Hace batch UPDATE de
filas sin embedding; idempotente y reanudable.

Uso:
  docker compose run --rm embed python -m etl.embed --limit 5000 --batch 64
"""
import argparse
import logging
import os

log = logging.getLogger("embed")
MODEL = os.getenv("EMB_MODEL", "paraphrase-multilingual-MiniLM-L12-v2")


def _text(r):
    return " ".join(x for x in
                    [r[0] or "", r[1] or "", r[2] or ""] if x)[:2000]


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--limit", type=int, default=5000)
    p.add_argument("--batch", type=int, default=64)
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    from sentence_transformers import SentenceTransformer
    from .db import get_conn, init_schema

    init_schema()
    log.info("cargando modelo %s ...", MODEL)
    model = SentenceTransformer(MODEL)
    done = 0
    while done < args.limit:
        n = min(args.batch * 8, args.limit - done)
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id_del_proceso, id_adjudicacion, codigoproveedor, valor_llave,"
                    " nombre_del_procedimiento, descripcion, entidad"
                    " FROM secop2_procesos WHERE embedding IS NULL LIMIT %s", (n,))
                rows = cur.fetchall()
        finally:
            conn.close()
        if not rows:
            break
        texts = [_text(r[4:]) for r in rows]
        vecs = model.encode(texts, batch_size=args.batch, show_progress_bar=False)
        conn = get_conn()
        try:
            with conn:
                with conn.cursor() as cur:
                    for r, v in zip(rows, vecs):
                        cur.execute(
                            "UPDATE secop2_procesos SET embedding = %s"
                            " WHERE id_del_proceso=%s AND id_adjudicacion=%s"
                            " AND codigoproveedor=%s AND valor_llave=%s",
                            (list(map(float, v)), r[0], r[1], r[2], r[3]),
                        )
        finally:
            conn.close()
        done += len(rows)
        log.info("embeddings: %d/%d", done, args.limit)
    log.info("done: %d", done)


if __name__ == "__main__":
    main()
