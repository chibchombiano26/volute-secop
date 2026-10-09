"""API lectura SECOP 2026 + Scalar docs.

Corre en Docker (python 3.11): el SDK MCP exige >=3.10.
"""
from fastapi import FastAPI, HTTPException, Query, Request
from scalar_fastapi import add_scalar_reference

from . import queries
from . import live as liveq
from .auth import ApiKeyMiddleware

app = FastAPI(title="Volute SECOP API",
              description="Lectura de licitaciones abiertas SECOP II (2026) y documentos. Solo-lectura.",
              version="1.0.0",
              docs_url=None, redoc_url=None)

app.add_middleware(ApiKeyMiddleware)
add_scalar_reference(app, route="/scalar")


def _with_pages(request: Request, res: dict) -> dict:
    """Agrega next/prev como URLs absolutas (None si no hay mas)."""
    import urllib.parse

    from .cache import last_hit
    out = dict(res)

    def url(offset):
        qp = dict(request.query_params)
        qp["offset"] = str(offset)
        return str(request.url.replace(
            query=urllib.parse.urlencode(qp)))

    out["next"] = url(res["next_offset"]) if res.get("next_offset") is not None else None
    out["prev"] = url(res["prev_offset"]) if res.get("prev_offset") is not None else None
    out["cached"] = last_hit.get(False)
    return out


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/licitaciones", tags=["licitaciones"])
def licitaciones(request: Request, estado: str = Query(None, description="Ej. Publicado"),
                 entidad: str = Query(None),
                 modalidad: str = Query(None),
                 departamento: str = Query(None),
                 q: str = Query(None, description="Texto en nombre/descripcion"),
                 desde: str = Query(None, description="YYYY-MM-DD"),
                 hasta: str = Query(None, description="YYYY-MM-DD"),
                 limit: int = Query(20, le=100),
                 offset: int = Query(0, ge=0),
                 fresh: bool = Query(False, description="1 = saltar cache")):
    res = queries.buscar_licitaciones(estado, entidad, modalidad, departamento,
                                      q, desde, hasta, limit, offset, fresh=fresh)
    return _with_pages(request, res)


@app.get("/licitaciones-semantic", tags=["licitaciones"])
def licitaciones_semantic(request: Request, q: str, limit: int = Query(20, le=100),
                          offset: int = Query(0, ge=0),
                          fresh: bool = Query(False, description="1 = saltar cache")):
    return _with_pages(request, queries.buscar_semantico(q, limit, offset, fresh=fresh))


@app.get("/licitaciones/{id_del_proceso}", tags=["licitaciones"])
def licitacion(id_del_proceso: str):
    rows = queries.detalle_licitacion(id_del_proceso)
    if not rows:
        raise HTTPException(404, "no existe")
    return {"id_del_proceso": id_del_proceso, "filas": rows}


@app.get("/documentos/search", tags=["documentos"])
def documentos_search(request: Request, q: str, entidad: str = Query(None),
                      limit: int = Query(20, le=100),
                      offset: int = Query(0, ge=0),
                      fresh: bool = Query(False, description="1 = saltar cache")):
    res = queries.buscar_documentos(q, entidad, limit, offset, fresh=fresh)
    res["q"] = q
    return _with_pages(request, res)


@app.get("/documentos/{id_documento}", tags=["documentos"])
def documento(id_documento: int):
    row = queries.detalle_documento(id_documento)
    if not row:
        raise HTTPException(404, "no existe")
    return row


@app.get("/live/licitaciones", tags=["live"])
def live_licitaciones(request: Request, estado: str = Query(None), entidad: str = Query(None),
                      modalidad: str = Query(None), departamento: str = Query(None),
                      q: str = Query(None), desde: str = Query(None),
                      hasta: str = Query(None), limit: int = Query(20, le=100),
                      offset: int = Query(0, ge=0),
                      fresh: bool = Query(False, description="1 = saltar cache")):
    res = liveq.live_licitaciones(estado, entidad, modalidad, departamento,
                                  q, desde, hasta, limit, offset, fresh=fresh)
    return _with_pages(request, res)


@app.get("/live/documentos", tags=["live"])
def live_documentos(request: Request, q: str, entidad: str = Query(None),
                    limit: int = Query(20, le=100),
                    offset: int = Query(0, ge=0),
                    fresh: bool = Query(False, description="1 = saltar cache")):
    res = liveq.live_documentos(q, entidad, limit, offset, fresh=fresh)
    return _with_pages(request, res)
