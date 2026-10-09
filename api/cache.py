"""Cache en memoria con TTL (por proceso). Sin dependencias.

Env: CACHE_TTL segundos (default 300), CACHE_DISABLE=1 lo apaga.
"""
import os
import threading
import time
from contextvars import ContextVar

TTL = int(os.getenv("CACHE_TTL", "600"))
DISABLED = os.getenv("CACHE_DISABLE", "0") == "1"
MAX_ENTRIES = 512

_store = {}
_lock = threading.Lock()
last_hit = ContextVar("cache_last_hit", default=False)


def _key(fn, args, kwargs):
    parts = [fn]
    parts += [repr(a) for a in args]
    for k in sorted(kwargs):
        if k == "fresh":
            continue
        parts.append("%s=%r" % (k, kwargs[k]))
    return "|".join(parts)


def cached(fn):
    """Decorador: usa kwargs fresh=True para saltar el cache."""
    name = fn.__name__

    def wrapper(*args, **kwargs):
        fresh = kwargs.get("fresh", False)
        if DISABLED or TTL <= 0:
            return fn(*args, **kwargs)
        key = _key(name, args, kwargs)
        now = time.time()
        if not fresh:
            with _lock:
                hit = _store.get(key)
            if hit and hit[0] > now:
                last_hit.set(True)
                return hit[1]
        val = fn(*args, **kwargs)
        last_hit.set(False)
        with _lock:
            if len(_store) >= MAX_ENTRIES:
                _store.pop(next(iter(_store)))
            _store[key] = (now + TTL, val)
        return val

    wrapper.__name__ = name
    return wrapper
