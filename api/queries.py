"""Queries read-only sobre Postgres SECOP. Compartidas por API y MCP."""
import os

import psycopg2
import psycopg2.extras

from .cache import cached

DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://secop:secop@localhost:5432/secop"
)
MAX_LIMIT = 100


def q(conn, sql, params=()):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SET LOCAL statement_timeout = '15s'")
        cur.execute(sql, params)
        return cur.fetchall()


def conn():
    return psycopg2.connect(DATABASE_URL)


LIC_COLS = ("id_del_proceso, referencia_del_proceso, entidad, nit_entidad,"
            " departamento_entidad, ciudad_entidad, nombre_del_procedimiento,"
            " fase, estado_del_procedimiento, modalidad_de_contratacion,"
            " tipo_de_contrato, precio_base, valor_total_adjudicacion,"
            " fecha_publicacion, fecha_ultima_publicacion, fecha_adjudicacion,"
            " nombre_proveedor, nit_proveedor, url_proceso")


@cached
def buscar_licitaciones(estado=None, entidad=None, modalidad=None,
                        departamento=None, qtexto=None, desde=None, hasta=None,
                        limit=20, offset=0, fresh=False):
    limit = max(1, min(int(limit or 20), MAX_LIMIT))
    offset = max(0, int(offset or 0))
    clauses, params = [], []
    if estado:
        clauses.append("estado_del_procedimiento = %s")
        params.append(estado)
    if entidad:
        clauses.append("entidad ILIKE %s")
        params.append("%%%s%%" % entidad)
    if modalidad:
        clauses.append("modalidad_de_contratacion ILIKE %s")
        params.append("%%%s%%" % modalidad)
    if departamento:
        clauses.append("departamento_entidad ILIKE %s")
        params.append("%%%s%%" % departamento)
    if desde:
        clauses.append("fecha_publicacion >= %s")
        params.append(desde)
    if hasta:
        clauses.append("fecha_publicacion <= %s")
        params.append(hasta)
    if qtexto:
        clauses.append("(nombre_del_procedimiento ILIKE %s OR descripcion ILIKE %s)")
        params += ["%%%s%%" % qtexto, "%%%s%%" % qtexto]
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    c = conn()
    try:
        total = q(c, "SELECT count(*) AS n FROM secop2_procesos %s" % where, params)[0]["n"]
        rows = q(c, "SELECT %s FROM secop2_procesos %s ORDER BY fecha_publicacion DESC, id_del_proceso ASC"
                 " LIMIT %s OFFSET %s" % (LIC_COLS, where, limit, offset), params)
        data = [dict(r) for r in rows]
        return {"total": total, "limit": limit, "offset": offset,
                "next_offset": offset + limit if offset + limit < total else None,
                "prev_offset": offset - limit if offset - limit >= 0 else None,
                "data": data}
    finally:
        c.close()


@cached
def detalle_licitacion(id_del_proceso, fresh=False):
    c = conn()
    try:
        rows = q(c, "SELECT %s, descripcion FROM secop2_procesos"
                 " WHERE id_del_proceso = %%s" % LIC_COLS, (id_del_proceso,))
        return [dict(r) for r in rows]
    finally:
        c.close()


@cached
def buscar_documentos(qtexto, entidad=None, limit=20, offset=0, fresh=False):
    limit = max(1, min(int(limit or 20), MAX_LIMIT))
    offset = max(0, int(offset or 0))
    c = conn()
    try:
        params = [qtexto, qtexto]
        extra, p2 = "", []
        if entidad:
            extra = "AND entidad ILIKE %s"
            p2 = ["%%%s%%" % entidad]
        rows = q(c,
                 "SELECT id_documento, proceso, numero_contrato, entidad,"
                 " nombre_archivo, tamanno, extension, fecha_carga,"
                 " ts_rank(texto_tsv, plainto_tsquery('spanish', %%s)) AS rank"
                 " FROM secop_documentos"
                 " WHERE texto_tsv @@ plainto_tsquery('spanish', %%s) %s"
                 " ORDER BY rank DESC LIMIT %s OFFSET %s" % (extra, limit, offset),
                 params + p2)
        data = [dict(r) for r in rows]
        return {"limit": limit, "offset": offset,
                "next_offset": offset + limit if len(data) == limit else None,
                "prev_offset": offset - limit if offset - limit >= 0 else None,
                "data": data}
    finally:
        c.close()


def detalle_documento(id_documento):
    c = conn()
    try:
        rows = q(c, "SELECT id_documento, proceso, numero_contrato, entidad,"
                 " nombre_archivo, tamanno, extension, descripcion, fecha_carga,"
                 " left(texto, 4000) AS texto_preview FROM secop_documentos"
                 " WHERE id_documento = %s", (id_documento,))
        return dict(rows[0]) if rows else None
    finally:
        c.close()


@cached
def buscar_semantico(qtexto, limit=20, offset=0, fresh=False):
    from .embeddings import embed
    limit = max(1, min(int(limit or 20), MAX_LIMIT))
    offset = max(0, int(offset or 0))
    vec = embed(qtexto)
    c = conn()
    try:
        rows = q(c, "SELECT %s, 1 - (embedding <=> %%s::vector) AS score"
                 " FROM secop2_procesos WHERE embedding IS NOT NULL"
                 " ORDER BY embedding <=> %%s::vector, id_del_proceso LIMIT %s OFFSET %s"
                 % (LIC_COLS, limit, offset), (vec, vec))
        data = [dict(r) for r in rows]
        return {"limit": limit, "offset": offset,
                "next_offset": offset + limit if len(data) == limit else None,
                "prev_offset": offset - limit if offset - limit >= 0 else None,
                "data": data}
    finally:
        c.close()
