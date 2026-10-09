-- SECOP incremental ingest schema (Postgres)
-- Idempotente: puede ejecutarse multiples veces.

CREATE TABLE IF NOT EXISTS ingest_state (
    dataset_key     TEXT PRIMARY KEY,
    socrata_id      TEXT NOT NULL,
    watermark_column TEXT NOT NULL,
    last_value      TEXT,              -- ISO timestamp string, NULL = nunca sincronizado
    last_pk         TEXT,              -- :id fisico Socrata de la ultima fila (cursor)
    rows_synced     BIGINT NOT NULL DEFAULT 0,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- Migracion para DBs ya creadas:
ALTER TABLE ingest_state ADD COLUMN IF NOT EXISTS last_pk TEXT;

-- Progreso del backfill historico por ventanas (reanudable).
-- filtro distingue corridas (ej. 'open-only' vs '') para no mezclar.
CREATE TABLE IF NOT EXISTS backfill_progress (
    dataset_key  TEXT NOT NULL,
    window_start TEXT NOT NULL,
    window_end   TEXT NOT NULL,
    filtro       TEXT NOT NULL DEFAULT '',
    rows_synced  BIGINT NOT NULL DEFAULT 0,
    done_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (dataset_key, window_start, window_end, filtro)
);
ALTER TABLE backfill_progress ADD COLUMN IF NOT EXISTS filtro TEXT NOT NULL DEFAULT '';

-- SECOP II Procesos (p6dx-8zbt).
-- PK compuesta: un proceso trae una fila por adjudicatario (ej. acuerdos marco
-- con 25+ adjudicaciones) y hasta el mismo (proceso, adjudicacion, proveedor)
-- se repite con distinto valor (lotes sin id propio).
CREATE TABLE IF NOT EXISTS secop2_procesos (
    id_del_proceso            TEXT NOT NULL,
    id_adjudicacion           TEXT NOT NULL DEFAULT '',
    codigoproveedor           TEXT NOT NULL DEFAULT '',
    valor_llave               TEXT NOT NULL DEFAULT '',
    referencia_del_proceso    TEXT,
    entidad                   TEXT,
    nit_entidad               TEXT,
    departamento_entidad      TEXT,
    ciudad_entidad            TEXT,
    nombre_del_procedimiento  TEXT,
    descripcion               TEXT,
    fase                      TEXT,
    estado_del_procedimiento  TEXT,
    modalidad_de_contratacion TEXT,
    tipo_de_contrato          TEXT,
    precio_base               NUMERIC,
    valor_total_adjudicacion  NUMERIC,
    fecha_publicacion         TIMESTAMPTZ,
    fecha_ultima_publicacion  TIMESTAMPTZ,
    fecha_adjudicacion        TIMESTAMPTZ,
    nombre_proveedor          TEXT,
    nit_proveedor             TEXT,
    url_proceso               TEXT,
    raw                       JSONB NOT NULL,
    created_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (id_del_proceso, id_adjudicacion, codigoproveedor, valor_llave)
);
CREATE INDEX IF NOT EXISTS idx_s2p_watermark ON secop2_procesos (fecha_ultima_publicacion);
CREATE INDEX IF NOT EXISTS idx_s2p_estado ON secop2_procesos (estado_del_procedimiento);
CREATE INDEX IF NOT EXISTS idx_s2p_modalidad ON secop2_procesos (modalidad_de_contratacion);
CREATE INDEX IF NOT EXISTS idx_s2p_entidad ON secop2_procesos (entidad);

-- SECOP II Contratos (jbjy-vk9h). PK natural: id_contrato (CO1.PCCNTR.xxx)
-- NOTA: ultima_actualizacion viene con muchos nulos en Socrata,
-- por eso el watermark por defecto es fecha_de_firma.
CREATE TABLE IF NOT EXISTS secop2_contratos (
    id_contrato          TEXT PRIMARY KEY,
    proceso_de_compra    TEXT,
    entidad              TEXT,
    nit_entidad          TEXT,
    departamento         TEXT,
    ciudad               TEXT,
    descripcion          TEXT,
    objeto               TEXT,
    tipo_de_contrato     TEXT,
    modalidad            TEXT,
    estado_contrato      TEXT,
    fecha_de_firma       TIMESTAMPTZ,
    valor_del_contrato   NUMERIC,
    proveedor            TEXT,
    documento_proveedor  TEXT,
    url_proceso          TEXT,
    ultima_actualizacion TIMESTAMPTZ,
    raw                  JSONB NOT NULL,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_s2c_firma ON secop2_contratos (fecha_de_firma);
CREATE INDEX IF NOT EXISTS idx_s2c_estado ON secop2_contratos (estado_contrato);

-- SECOP I Procesos (f789-7hwg). PK natural: uid (columna nativa del dataset)
CREATE TABLE IF NOT EXISTS secop1_procesos (
    uid                  TEXT PRIMARY KEY,
    numero_de_proceso     TEXT,
    numero_de_contrato    TEXT,
    numero_de_constancia  TEXT,
    entidad               TEXT,
    nit_entidad           TEXT,
    departamento_entidad  TEXT,
    municipio_entidad     TEXT,
    modalidad             TEXT,
    estado_del_proceso    TEXT,
    tipo_de_contrato      TEXT,
    objeto                TEXT,
    cuantia_proceso       NUMERIC,
    cuantia_contrato      NUMERIC,
    fecha_cargue          TIMESTAMPTZ,
    fecha_firma           TIMESTAMPTZ,
    contratista           TEXT,
    identificacion_contratista TEXT,
    ultima_actualizacion  TIMESTAMPTZ,
    ruta_proceso          TEXT,
    raw                   JSONB NOT NULL,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_s1p_watermark ON secop1_procesos (ultima_actualizacion);
CREATE INDEX IF NOT EXISTS idx_s1p_entidad ON secop1_procesos (entidad);

-- Documentos SECOP II (datasets "Archivos Descarga", uno por rango temporal).
-- PK: id_documento. Los docs pre-contractuales (n_mero_de_contrato NULL) son
-- pliegos/adendas/observaciones = adjuntos de licitaciones abiertas.
-- NOTA: doc.proceso es CO1.BDOS.xxx; procesos.id_del_proceso es CO1.REQ.xxx
-- y datos abiertos no trae el puente REQ<->BDOS (el portal lo bloquea con
-- captcha). El agente consulta por entidad/texto (FTS), no por REQ.
CREATE TABLE IF NOT EXISTS secop_documentos (
    id_documento      BIGINT PRIMARY KEY,
    proceso           TEXT,
    numero_contrato   TEXT,
    nombre_archivo    TEXT,
    tamanno           BIGINT,
    extension         TEXT,
    descripcion       TEXT,
    fecha_carga       TIMESTAMPTZ,
    entidad           TEXT,
    nit_entidad       TEXT,
    doc_url           TEXT,
    storage_path      TEXT,
    sha256            TEXT,
    estado            TEXT NOT NULL DEFAULT 'pending',
    texto             TEXT,
    texto_tsv         TSVECTOR,
    fail_count        INT NOT NULL DEFAULT 0,
    downloaded_at     TIMESTAMPTZ,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    raw               JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_doc_fecha ON secop_documentos (fecha_carga);
CREATE INDEX IF NOT EXISTS idx_doc_estado ON secop_documentos (estado);
CREATE INDEX IF NOT EXISTS idx_doc_proceso ON secop_documentos (proceso);
CREATE INDEX IF NOT EXISTS idx_doc_contrato ON secop_documentos (numero_contrato);
CREATE INDEX IF NOT EXISTS idx_doc_entidad ON secop_documentos (entidad);
CREATE INDEX IF NOT EXISTS idx_doc_fts ON secop_documentos USING GIN (texto_tsv);

-- Busqueda semantica (pgvector). Modelo: paraphrase-multilingual-MiniLM-L12-v2 (384 dims).
CREATE EXTENSION IF NOT EXISTS vector;
ALTER TABLE secop2_procesos ADD COLUMN IF NOT EXISTS embedding vector(384);
CREATE INDEX IF NOT EXISTS idx_s2p_emb ON secop2_procesos
  USING hnsw (embedding vector_cosine_ops) WITH (m = 16, ef_construction = 64);
