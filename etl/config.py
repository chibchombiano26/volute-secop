"""Config central del ETL SECOP. Todo via env con defaults sanos."""
import os

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass


def env(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None or v == "" else v


SOCRATA_DOMAIN = env("SOCRATA_DOMAIN", "www.datos.gov.co")
SOCRATA_APP_TOKEN = env("SOCRATA_APP_TOKEN", "")
SOCRATA_TIMEOUT = int(env("SOCRATA_TIMEOUT", "40"))
PAGE_SIZE = int(env("PAGE_SIZE", "1000"))
DATABASE_URL = env(
    "DATABASE_URL",
    "postgresql://secop:secop@localhost:5432/secop",
)

# dataset_key -> definicion
# watermark: columna usada para ingesta incremental (debe existir en Socrata)
# pk: identidad logica de fila (puede ser compuesta: SECOP II repite
#     id_del_proceso una vez por adjudicatario; el valor difiere por lote).
#
# ESTADOS_ABIERTOS: filtro "licitaciones abiertas" (todo menos
# Seleccionado/Cancelado). El daily SIN filtro trae tambien los cambios
# de estado (ej. Publicado -> Seleccionado) para no dejar filas stale.
ESTADOS_ABIERTOS = [
    "Borrador",
    "Publicado",
    "Evaluación",
    "Abierto",
    "Suspendido",
    "En aprobación",
    "Aprobado",
]

OPEN_WHERE = "estado_del_procedimiento IN (%s)" % ", ".join(
    "'%s'" % e for e in ESTADOS_ABIERTOS
)
DATASETS = {
    "secop2_procesos": {
        "socrata_id": "p6dx-8zbt",
        "table": "secop2_procesos",
        "pk": ["id_del_proceso", "id_adjudicacion", "codigoproveedor",
               "valor_llave"],
        "watermark": "fecha_de_ultima_publicaci",
    },
    "secop2_contratos": {
        "socrata_id": "jbjy-vk9h",
        "table": "secop2_contratos",
        "pk": ["id_contrato"],
        # ultima_actualizacion tiene demasiados nulos -> usar fecha_de_firma
        "watermark": "fecha_de_firma",
    },
    "secop1_procesos": {
        "socrata_id": "f789-7hwg",
        "table": "secop1_procesos",
        "pk": ["uid"],
        "watermark": "ultima_actualizacion",
    },
    # Documentos SECOP II: SOLO desde 2025 (activas). Los slices historicos
    # (2024, 2023, 2022, hasta-2021) estan fuera de alcance a proposito.
    "secop2_docs_2025": {
        "socrata_id": "dmgg-8hin",
        "table": "secop_documentos",
        "pk": ["id_documento"],
        "watermark": "fecha_carga",
    },
}

# Solo pre-contractuales (sin contrato): adjuntos de licitaciones abiertas.
DOCS_PRE_WHERE = "n_mero_de_contrato IS NULL"
