"""Tests sin DB: SoQL builder, keyset cursor y transforms/upsert-sql."""
import unittest

from etl.socrata import SocrataClient
from etl import db


class TestSoql(unittest.TestCase):
    def test_incremental_query(self):
        q = SocrataClient.build_incremental_query(
            "fecha_de_ultima_publicaci",
            since="2026-10-05T00:00:00.000", limit=10)
        self.assertIn("fecha_de_ultima_publicaci IS NOT NULL", q)
        self.assertIn("fecha_de_ultima_publicaci > '2026-10-05T00:00:00.000'", q)
        self.assertIn("ORDER BY fecha_de_ultima_publicaci ASC, :id ASC", q)
        self.assertIn("LIMIT 10", q)

    def test_extra_where(self):
        q = SocrataClient.build_incremental_query(
            "fecha_de_ultima_publicaci",
            extra_where="estado_del_procedimiento='Publicado'", limit=5)
        self.assertIn("estado_del_procedimiento='Publicado'", q)


class TestTransforms(unittest.TestCase):
    def test_secop2_procesos_pk(self):
        r = {"id_del_proceso": "CO1.REQ.1", "entidad": "X",
             "urlproceso": {"url": "https://x"}, "precio_base": "100"}
        n = db.row_secop2_procesos(r)
        self.assertEqual(n["id_del_proceso"], "CO1.REQ.1")
        self.assertEqual(n["url_proceso"], "https://x")
        self.assertEqual(n["precio_base"], 100.0)

    def test_secop1_uid(self):
        r = {"uid": "abc", "cuantia_proceso": "", "ruta_proceso_en_secop_i": {"url": "u"}}
        n = db.row_secop1_procesos(r)
        self.assertEqual(n["uid"], "abc")
        self.assertIsNone(n["cuantia_proceso"])

    def test_num_garbage(self):
        self.assertIsNone(db._num("No Definido"))
        self.assertIsNone(db._num(""))
        self.assertIsNone(db._num(None))


class TestCursor(unittest.TestCase):
    def test_composite_keyset(self):
        # mismo dia + :id -> la 2da pagina no pierde filas del mismo dia
        q = SocrataClient.build_incremental_query(
            "fecha_de_ultima_publicaci",
            since="2026-10-07T00:00:00.000", since_id="row-abc", limit=10)
        self.assertIn(":id > 'row-abc'", q)
        self.assertIn("ORDER BY fecha_de_ultima_publicaci ASC, :id ASC", q)
        self.assertIn("*, :id", q)

    def test_pkey(self):
        from etl.db import _pkey
        raw = {"id_del_proceso": "CO1.REQ.1", "id_adjudicacion": None,
               "codigoproveedor": "7001", "valor_total_adjudicacion": "100"}
        self.assertEqual(_pkey("secop2_procesos", raw),
                         ("CO1.REQ.1", "", "7001", "100"))

    def test_doc_transform(self):
        r = {"id_documento": "757263580", "proceso": "CO1.BDOS.1",
             "n_mero_de_contrato": None, "nombre_archivo": "a.pdf",
             "tamanno_archivo": "4260384", "extensi_n": "PDF",
             "url_descarga_documento": {"url": "https://x"}}
        n = db.row_secop_documentos(r)
        self.assertEqual(n["id_documento"], 757263580)
        self.assertEqual(n["extension"], "pdf")
        self.assertEqual(n["tamanno"], 4260384)
        self.assertEqual(db._pkey("secop2_docs_2025", r), ("757263580",))

    def test_rewind(self):
        from etl.sync import _rewind_days
        self.assertEqual(_rewind_days("2026-10-07T00:00:00.000", 2),
                         "2026-10-05T00:00:00.000")

    def test_windows(self):
        from etl.backfill import month_windows
        w = list(month_windows("2026-01", "2026-03"))
        self.assertEqual(len(w), 2)
        self.assertTrue(w[0][0].startswith("2026-01-01"))
        self.assertTrue(w[0][1].startswith("2026-02-01"))


if __name__ == "__main__":
    unittest.main()
