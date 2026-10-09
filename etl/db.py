"""Capa DB Postgres: schema, watermarks y upserts idempotentes."""
import json
import os

import psycopg2
import psycopg2.extras

from . import config


def get_conn():
    return psycopg2.connect(config.DATABASE_URL)


def init_schema():
    path = os.path.join(os.path.dirname(__file__), "schema.sql")
    with open(path, encoding="utf-8") as f:
        sql = f.read()
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql)
        migrate(conn)
    finally:
        conn.close()


def _pk_columns(conn, table):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT kcu.column_name FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            WHERE tc.table_name=%s AND tc.constraint_type='PRIMARY KEY'
            ORDER BY kcu.ordinal_position
            """,
            (table,),
        )
        return [r[0] for r in cur.fetchall()]


def migrate(conn):
    """Migra secop2_procesos de PK simple a compuesta (una sola vez).

    Las filas viejas colapsaban un proceso a 1 fila; se truncan y sus
    ventanas se marcan pendientes para re-ingerir con el grano correcto.
    Es data re-obtenible de la API (no hay input manual).
    """
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM information_schema.tables WHERE table_name='secop2_procesos'"
            )
            if not cur.fetchone():
                return
    if _pk_columns(conn, "secop2_procesos") == ["id_del_proceso"]:
        with conn:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE secop2_procesos")
                cur.execute(
                    "DELETE FROM backfill_progress WHERE dataset_key='secop2_procesos'"
                )
                cur.execute(
                    "ALTER TABLE secop2_procesos "
                    "ADD COLUMN IF NOT EXISTS id_adjudicacion TEXT NOT NULL DEFAULT '', "
                    "ADD COLUMN IF NOT EXISTS codigoproveedor TEXT NOT NULL DEFAULT '', "
                    "ADD COLUMN IF NOT EXISTS valor_llave TEXT NOT NULL DEFAULT ''"
                )
                cur.execute("ALTER TABLE secop2_procesos DROP CONSTRAINT secop2_procesos_pkey")
                cur.execute(
                    "ALTER TABLE secop2_procesos ADD PRIMARY KEY "
                    "(id_del_proceso, id_adjudicacion, codigoproveedor, valor_llave)"
                )
                cur.execute(
                    "UPDATE ingest_state SET last_pk=NULL WHERE dataset_key='secop2_procesos'"
                )
    # backfill_progress: PK vieja (sin filtro) -> PK con filtro
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                "ALTER TABLE backfill_progress ADD COLUMN IF NOT EXISTS filtro TEXT NOT NULL DEFAULT ''"
            )
    if _pk_columns(conn, "backfill_progress") == [
        "dataset_key", "window_start", "window_end",
    ]:
        with conn:
            with conn.cursor() as cur:
                cur.execute("ALTER TABLE backfill_progress DROP CONSTRAINT backfill_progress_pkey")
                cur.execute(
                    "ALTER TABLE backfill_progress ADD PRIMARY KEY "
                    "(dataset_key, window_start, window_end, filtro)"
                )


def get_watermark(conn, dataset_key):
    with conn.cursor() as cur:
        cur.execute("SELECT last_value FROM ingest_state WHERE dataset_key=%s", (dataset_key,))
        r = cur.fetchone()
        return r[0] if r else None


def get_cursor(conn, dataset_key):
    """Devuelve (last_value, last_row_id) para reanudar sin perder filas.

    last_row_id es el :id fisico de Socrata de la ultima fila consumida.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT last_value, last_pk FROM ingest_state WHERE dataset_key=%s",
                    (dataset_key,))
        r = cur.fetchone()
        if not r:
            return None, None
        return r[0], r[1]


def set_watermark(conn, dataset_key, socrata_id, watermark_col, last_value, rows=0,
                  last_pk=None):
    """Avanza el watermark solo hacia adelante (monotonico).

    Necesario para backfill por ventanas viejas: sincronizar 2015 no debe
    regresar el cursor global si ya esta en 2026.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ingest_state(dataset_key, socrata_id, watermark_column, last_value, last_pk, rows_synced, updated_at)
            VALUES (%s,%s,%s,%s,%s,%s, now())
            ON CONFLICT (dataset_key) DO UPDATE SET
              last_value = CASE
                WHEN ingest_state.last_value IS NULL THEN EXCLUDED.last_value
                WHEN EXCLUDED.last_value IS NULL THEN ingest_state.last_value
                WHEN EXCLUDED.last_value > ingest_state.last_value THEN EXCLUDED.last_value
                ELSE ingest_state.last_value END,
              last_pk = CASE
                WHEN EXCLUDED.last_value IS NULL THEN ingest_state.last_pk
                WHEN ingest_state.last_value IS NULL THEN EXCLUDED.last_pk
                WHEN EXCLUDED.last_value > ingest_state.last_value THEN EXCLUDED.last_pk
                -- mismo watermark: gana la ultima escritura (los batches llegan
                -- en orden y el :id no es comparable lexicograficamente)
                WHEN EXCLUDED.last_value = ingest_state.last_value THEN EXCLUDED.last_pk
                ELSE ingest_state.last_pk END,
              rows_synced = ingest_state.rows_synced + EXCLUDED.rows_synced,
              updated_at = now()
            """,
            (dataset_key, socrata_id, watermark_col, last_value, last_pk, rows),
        )


def mark_window_done(conn, dataset_key, window_start, window_end, rows, tag=""):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO backfill_progress(dataset_key, window_start, window_end, filtro, rows_synced, done_at)
            VALUES (%s,%s,%s,%s,%s, now())
            ON CONFLICT (dataset_key, window_start, window_end, filtro) DO UPDATE SET
              rows_synced = EXCLUDED.rows_synced, done_at = now()
            """,
            (dataset_key, window_start, window_end, tag, rows),
        )


def is_window_done(conn, dataset_key, window_start, window_end, tag=""):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM backfill_progress WHERE dataset_key=%s AND window_start=%s AND window_end=%s AND filtro=%s",
            (dataset_key, window_start, window_end, tag),
        )
        return cur.fetchone() is not None


# ---- transforms: Socrata row -> fila normalizada ----

def _url(v):
    if isinstance(v, dict):
        return v.get("url")
    return v


def _num(v):
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def row_secop2_procesos(r):
    return {
        "id_del_proceso": r.get("id_del_proceso"),
        "id_adjudicacion": r.get("id_adjudicacion"),
        "codigoproveedor": r.get("codigoproveedor"),
        "referencia_del_proceso": r.get("referencia_del_proceso"),
        "entidad": r.get("entidad"),
        "nit_entidad": r.get("nit_entidad"),
        "departamento_entidad": r.get("departamento_entidad"),
        "ciudad_entidad": r.get("ciudad_entidad"),
        "nombre_del_procedimiento": r.get("nombre_del_procedimiento"),
        "descripcion": r.get("descripci_n_del_procedimiento"),
        "fase": r.get("fase"),
        "estado_del_procedimiento": r.get("estado_del_procedimiento"),
        "modalidad_de_contratacion": r.get("modalidad_de_contratacion"),
        "tipo_de_contrato": r.get("tipo_de_contrato"),
        "precio_base": _num(r.get("precio_base")),
        "valor_total_adjudicacion": _num(r.get("valor_total_adjudicacion")),
        "valor_llave": ("" if r.get("valor_total_adjudicacion") is None
                        else str(r.get("valor_total_adjudicacion"))),
        "fecha_publicacion": r.get("fecha_de_publicacion_del"),
        "fecha_ultima_publicacion": r.get("fecha_de_ultima_publicaci"),
        "fecha_adjudicacion": r.get("fecha_adjudicacion"),
        "nombre_proveedor": r.get("nombre_del_proveedor"),
        "nit_proveedor": r.get("nit_del_proveedor_adjudicado"),
        "url_proceso": _url(r.get("urlproceso")),
        "raw": json.dumps(r, ensure_ascii=False),
    }


def row_secop2_contratos(r):
    return {
        "id_contrato": r.get("id_contrato"),
        "proceso_de_compra": r.get("proceso_de_compra"),
        "entidad": r.get("nombre_entidad"),
        "nit_entidad": r.get("nit_entidad"),
        "departamento": r.get("departamento"),
        "ciudad": r.get("ciudad"),
        "descripcion": r.get("descripcion_del_proceso"),
        "objeto": r.get("objeto_del_contrato"),
        "tipo_de_contrato": r.get("tipo_de_contrato"),
        "modalidad": r.get("modalidad_de_contratacion"),
        "estado_contrato": r.get("estado_contrato"),
        "fecha_de_firma": r.get("fecha_de_firma"),
        "valor_del_contrato": _num(r.get("valor_del_contrato")),
        "proveedor": r.get("proveedor_adjudicado"),
        "documento_proveedor": r.get("documento_proveedor"),
        "url_proceso": _url(r.get("urlproceso")),
        "ultima_actualizacion": r.get("ultima_actualizacion"),
        "raw": json.dumps(r, ensure_ascii=False),
    }


def row_secop1_procesos(r):
    return {
        "uid": r.get("uid"),
        "numero_de_proceso": r.get("numero_de_proceso"),
        "numero_de_contrato": r.get("numero_de_contrato"),
        "numero_de_constancia": r.get("numero_de_constancia"),
        "entidad": r.get("nombre_entidad"),
        "nit_entidad": r.get("nit_de_la_entidad"),
        "departamento_entidad": r.get("departamento_entidad"),
        "municipio_entidad": r.get("municipio_entidad"),
        "modalidad": r.get("modalidad_de_contratacion"),
        "estado_del_proceso": r.get("estado_del_proceso"),
        "tipo_de_contrato": r.get("tipo_de_contrato"),
        "objeto": r.get("objeto_del_contrato_a_la"),
        "cuantia_proceso": _num(r.get("cuantia_proceso")),
        "cuantia_contrato": _num(r.get("cuantia_contrato")),
        "fecha_cargue": r.get("fecha_de_cargue_en_el_secop"),
        "fecha_firma": r.get("fecha_de_firma_del_contrato"),
        "contratista": r.get("nom_razon_social_contratista"),
        "identificacion_contratista": r.get("identificacion_del_contratista"),
        "ultima_actualizacion": r.get("ultima_actualizacion"),
        "ruta_proceso": _url(r.get("ruta_proceso_en_secop_i")),
        "raw": json.dumps(r, ensure_ascii=False),
    }


def row_secop_documentos(r):
    u = r.get("url_descarga_documento")
    return {
        "id_documento": int(r.get("id_documento")) if r.get("id_documento") not in (None, "") else None,
        "proceso": r.get("proceso"),
        "numero_contrato": r.get("n_mero_de_contrato"),
        "nombre_archivo": r.get("nombre_archivo"),
        "tamanno": (None if r.get("tamanno_archivo") in (None, "")
                    else int(float(r.get("tamanno_archivo")))),
        "extension": (r.get("extensi_n") or "").lower(),
        "descripcion": r.get("descripci_n"),
        "fecha_carga": r.get("fecha_carga"),
        "entidad": r.get("entidad"),
        "nit_entidad": None if r.get("nit_entidad") in (None, "") else str(r.get("nit_entidad")),
        "doc_url": _url(u),
        "raw": json.dumps(r, ensure_ascii=False),
    }


TRANSFORMS = {
    "secop2_procesos": row_secop2_procesos,
    "secop2_contratos": row_secop2_contratos,
    "secop1_procesos": row_secop1_procesos,
    "secop2_docs_2025": row_secop_documentos,
}


def _pkey(dataset_key, raw):
    """Identidad logica de fila desde valores CRUDOS (string estable).

    Se usa el crudo y no el normalizado para que la llave sea estable
    (ej. numericos no cambian de formato entre corridas).
    """
    # columna llave -> columna cruda (valor_llave es el string crudo del valor)
    remap = {"valor_llave": "valor_total_adjudicacion"}
    parts = []
    for col in config.DATASETS[dataset_key]["pk"]:
        v = raw.get(remap.get(col, col))
        parts.append("" if v is None else str(v))
    return tuple(parts)


def upsert_batch(conn, dataset_key, rows):
    """Upsert idempotente: re-correr el mismo batch no duplica.

    Tambien deduplica dentro del batch (la fuente trae filas byte-identicas
    repetidas, ej. SECOP I uid duplicado) quedandose con la ultima.
    """
    table = config.DATASETS[dataset_key]["table"]
    pk_cols = config.DATASETS[dataset_key]["pk"]
    transform = TRANSFORMS[dataset_key]
    by_key = {}
    skipped = 0
    for r in rows:
        key = _pkey(dataset_key, r)
        if not any(key):
            skipped += 1
            continue  # sin PK no se puede deduplicar -> descartar
        by_key[key] = transform(r)
    normed = list(by_key.values())
    if not normed:
        return 0
    cols = list(normed[0].keys())
    # raw debe castear a jsonb
    values = []
    for n in normed:
        values.append(tuple(n[c] for c in cols))
    cols_sql = ", ".join(cols)
    # construir SET clause (todo menos pk; raw se pisa, updated_at=now())
    pk_set = set(pk_cols)
    set_sql = ", ".join(
        ["raw = EXCLUDED.raw::jsonb" if c == "raw"
         else "%s = EXCLUDED.%s" % (c, c) for c in cols if c not in pk_set]
    )
    tmpl = "(" + ", ".join(["%s"] * len(cols)) + ")"
    # ajustar el placeholder de raw a %s::jsonb
    # execute_values no soporta por-columna facil -> hacemos cast en el INSERT global:
    # insertamos raw como texto y postgres lo convierte via columna jsonb? No, mejor cast explicito:
    # Truco: nombrar columnas y usar ::jsonb en el SELECT de EXCLUDED ya esta; para el VALUES
    # necesitamos que psycopg envie el string y postgres lo parsee: funciona si la columna es jsonb.
    sql = (
        "INSERT INTO %s (%s) VALUES %%s "
        "ON CONFLICT (%s) DO UPDATE SET %s, updated_at = now()"
        % (table, cols_sql, ", ".join(pk_cols), set_sql)
    )
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, values, template=tmpl)
    return len(normed)
