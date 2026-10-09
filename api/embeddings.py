"""Modelo de embeddings (carga perezosa, compartido por API)."""
import os

MODEL = os.getenv("EMB_MODEL", "paraphrase-multilingual-MiniLM-L12-v2")
_model = None


def get_model():
    global _model
    if _model is None:
        from sentence_transformers import SentenceTransformer
        _model = SentenceTransformer(MODEL)
    return _model


def embed(text):
    return list(map(float, get_model().encode([text[:2000]])[0]))
