"""Cliente Socrata SODA con reintentos e iteracion incremental (keyset)."""
import time
import urllib.parse

import requests

from . import config


class SocrataClient:
    def __init__(self, domain=None, app_token=None, timeout=None):
        self.domain = domain or config.SOCRATA_DOMAIN
        self.app_token = app_token if app_token is not None else config.SOCRATA_APP_TOKEN
        self.timeout = timeout or config.SOCRATA_TIMEOUT
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "volute-secop-etl/1.0"})

        if self.app_token:
            self.session.headers.update({"X-App-Token": self.app_token})

    def _get(self, url, retries=5):
        last = None
        for attempt in range(retries):
            try:
                r = self.session.get(url, timeout=self.timeout)
                if r.status_code == 200:
                    return r.json()
                if r.status_code in (429, 500, 502, 503, 504):
                    last = RuntimeError("HTTP %s: %s" % (r.status_code, r.text[:300]))
                    time.sleep(2 ** attempt)
                    continue
                raise RuntimeError("Socrata HTTP %s: %s" % (r.status_code, r.text[:500]))
            except (requests.Timeout, requests.ConnectionError) as e:
                last = e
                time.sleep(2 ** attempt)
        raise RuntimeError("Socrata fallo tras %d intentos: %s" % (retries, last))

    def fetch(self, dataset_id, soql, limit=None):
        """Ejecuta una consulta SoQL y devuelve lista de dicts."""
        base = "https://%s/resource/%s.json" % (self.domain, dataset_id)
        params = {"$query": soql}
        url = base + "?" + urllib.parse.urlencode(params)
        rows = self._get(url)
        return rows if isinstance(rows, list) else []

    @staticmethod
    def _escape(v):
        return v.replace("'", "''")

    @staticmethod
    def build_incremental_query(watermark_col, since=None, until=None,
                                extra_where=None, limit=1000, select="*",
                                since_id=None):
        """SoQL incremental con keyset (watermark, :id).

        :id es el identificador fisico de fila de Socrata: unico, nunca nulo
        y comparable/ordenable. Sin esto se pierden filas porque el watermark
        es granularidad DIA (miles comparten fecha) y las PKs logicas se
        repiten (un proceso tiene una fila por adjudicatario).
        """
        if select.strip() == "*":
            select = "*, :id"
        elif ":id" not in select:
            select = select + ", :id"
        clauses = ["%s IS NOT NULL" % watermark_col]
        if since:
            s = SocrataClient._escape(since)
            if since_id:
                clauses.append(
                    "((%s > '%s') OR (%s = '%s' AND :id > '%s'))"
                    % (watermark_col, s, watermark_col, s,
                       SocrataClient._escape(since_id))
                )
            else:
                clauses.append("%s > '%s'" % (watermark_col, s))
        if until:
            clauses.append("%s <= '%s'" % (watermark_col, SocrataClient._escape(until)))
        if extra_where:
            clauses.append("(%s)" % extra_where)
        where = "WHERE " + " AND ".join(clauses)
        order = "ORDER BY %s ASC, :id ASC" % watermark_col
        return "SELECT %s %s %s LIMIT %d" % (select, where, order, limit)

    def iter_incremental(self, dataset_id, watermark_col, since=None,
                         until=None, extra_where=None, page_size=1000,
                         max_rows=None, select="*", since_id=None):
        """Genera batches con cursor (watermark, :id). Drena hasta agotar."""
        total = 0
        cursor_wm = since
        cursor_id = since_id
        while True:
            n = page_size
            if max_rows is not None:
                remaining = max_rows - total
                if remaining <= 0:
                    break
                n = min(n, remaining)
            soql = self.build_incremental_query(
                watermark_col, since=cursor_wm, until=until,
                extra_where=extra_where, limit=n, select=select,
                since_id=cursor_id,
            )
            batch = self.fetch(dataset_id, soql)
            if not batch:
                break
            yield batch, soql
            total += len(batch)
            # cursor = ultima fila (ORDER BY wm ASC, :id ASC)
            last = batch[-1]
            cursor_wm = last.get(watermark_col) or cursor_wm
            cursor_id = last.get(":id") or cursor_id
            if len(batch) < n:
                break
