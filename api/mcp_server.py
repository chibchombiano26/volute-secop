"""MCP Volute-SECOP: mismos datos del API como tools para agentes.

- STDIO (Claude Desktop/Cursor):  python -m api.mcp_server
- HTTP: montado en /mcp del API (Streamable HTTP).
SDK v1 (mcp>=1.28,<2). Corre en Docker python 3.11.
"""
import sys

from mcp.server.fastmcp import FastMCP

from . import queries

mcp = FastMCP("volute-secop", host="0.0.0.0", port=8001)


@mcp.tool()
def buscar_licitaciones(estado: str = "", entidad: str = "",
                        modalidad: str = "", departamento: str = "",
                        q: str = "", limit: int = 20, offset: int = 0, fresh: bool = False) -> dict:
    """Busca licitaciones abiertas SECOP II 2026 por estado, entidad,
    modalidad, departamento o texto. Devuelve total + filas."""
    return queries.buscar_licitaciones(estado or None, entidad or None,
                                       modalidad or None, departamento or None,
                                       q or None, None, None, limit, offset, fresh=fresh)


@mcp.tool()
def detalle_licitacion(id_del_proceso: str) -> dict:
    """Detalle completo de una licitacion por su ID (CO1.REQ.xxx).
    Puede traer varias filas (una por adjudicatario)."""
    rows = queries.detalle_licitacion(id_del_proceso)
    return {"id_del_proceso": id_del_proceso, "filas": rows}


@mcp.tool()
def buscar_documentos(q: str, entidad: str = "", limit: int = 10,
                        offset: int = 0, fresh: bool = False) -> dict:
    """Busca en el texto de los documentos (pliegos, anexos, estudios
    previos) por palabras clave, opcionalmente filtrando por entidad."""
    res = queries.buscar_documentos(q, entidad or None, limit, offset, fresh=fresh)
    res["q"] = q
    return res


@mcp.tool()
def buscar_semantico(q: str, limit: int = 10, offset: int = 0, fresh: bool = False) -> dict:
    """Busqueda semantica de licitaciones por significado, no por palabras
    exactas. Ej: 'arreglo de vias rurales' encuentra pavimentacion y
    mantenimiento vial aunque no compartan palabras."""
    res = queries.buscar_semantico(q, limit, offset, fresh=fresh)
    res["q"] = q
    return res


@mcp.tool()
def buscar_licitaciones_live(q: str = "", estado: str = "",
                             entidad: str = "", limit: int = 20,
                             offset: int = 0, fresh: bool = False) -> dict:
    """Busca licitaciones DIRECTO en datos.gov.co en vivo (historico completo
    y recien publicado, mas alla del snapshot local 2026)."""
    from . import live as liveq
    return liveq.live_licitaciones(estado or None, entidad or None, None,
                                   None, q or None, None, None, limit, offset, fresh=fresh)


@mcp.tool()
def buscar_documentos_live(q: str, entidad: str = "", limit: int = 10,
                           offset: int = 0, fresh: bool = False) -> dict:
    """Busca documentos DIRECTO en datos.gov.co en vivo por nombre o
    descripcion (historico completo)."""
    from . import live as liveq
    return liveq.live_documentos(q, entidad or None, limit, offset, fresh=fresh)


if __name__ == "__main__":
    if "--sse" in sys.argv:
        import uvicorn
        from starlette.applications import Starlette

        from .auth import ApiKeyMiddleware

        wrapped = Starlette()
        wrapped.add_middleware(ApiKeyMiddleware)
        wrapped.mount("/", mcp.sse_app())
        uvicorn.run(wrapped, host="0.0.0.0", port=8001)
    else:
        mcp.run()
