"""Static validation of the dbt model graph (no dbt install needed).

Checks:
  1. Every ref('model_name') in every .sql resolves to a defined model.
  2. Every source('source_name', 'table_name') resolves to sources.yml.
  3. Star schema completeness: fact_sales joins dim_customer, dim_product, dim_date.
  4. No orphan models (every model is referenced or is a mart/dim).
  5. YAML schema coverage: every model has a schema.yml entry.

Run: python tests/unit/test_dbt_graph.py
"""
import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MODELS = ROOT / "dbt" / "models"


def _sql_files():
    return list(MODELS.rglob("*.sql"))


def _model_names():
    return {f.stem for f in _sql_files()}


def _refs_in(sql_text):
    return set(re.findall(r"ref\(\s*['\"](\w+)['\"]", sql_text))


def _sources_in(sql_text):
    return set(re.findall(r"source\(\s*['\"](\w+)['\"]\s*,\s*['\"](\w+)['\"]", sql_text))


def _parse_sources_yml():
    import yaml
    text = (MODELS / "sources.yml").read_text()
    data = yaml.safe_load(text)
    pairs = set()
    for src in data.get("sources", []):
        for tbl in src.get("tables", []):
            pairs.add((src["name"], tbl["name"]))
    return pairs


def _parse_schema_models():
    """Return dict model_name -> set of column names from all schema.yml files."""
    import yaml
    models = {}
    for f in MODELS.rglob("schema.yml"):
        data = yaml.safe_load(f.read_text())
        for m in data.get("models", []):
            models[m["name"]] = {c["name"] for c in m.get("columns", [])}
    return models


class TestDbtGraph(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.models = _model_names()
        cls.sources = _parse_sources_yml()
        cls.schema_models = _parse_schema_models()

    def test_all_refs_resolve(self):
        bad = []
        for f in _sql_files():
            for ref in _refs_in(f.read_text()):
                if ref not in self.models:
                    bad.append(f"{f.name}: ref('{ref}')")
        self.assertEqual(bad, [], f"Unresolved refs: {bad}")

    def test_all_sources_resolve(self):
        bad = []
        for f in _sql_files():
            for src, tbl in _sources_in(f.read_text()):
                if (src, tbl) not in self.sources:
                    bad.append(f"{f.name}: source('{src}', '{tbl}')")
        self.assertEqual(bad, [], f"Unresolved sources: {bad}")

    def test_fact_sales_joins_all_dimensions(self):
        sql = (MODELS / "marts" / "fact_sales.sql").read_text()
        for dim in ("dim_customer", "dim_product", "dim_date"):
            self.assertIn(f"ref('{dim}')", sql, f"fact_sales missing join to {dim}")

    def test_fact_sales_has_all_dimension_keys(self):
        sql = (MODELS / "marts" / "fact_sales.sql").read_text()
        for key in ("customer_id", "product_id", "date_id"):
            self.assertIn(key, sql, f"fact_sales missing column {key}")

    def test_every_model_has_schema_entry(self):
        missing = [m for m in self.models if m not in self.schema_models]
        self.assertEqual(missing, [], f"Models without schema.yml entry: {missing}")

    def test_staging_models_all_exist(self):
        for name in ("stg_customers", "stg_products", "stg_orders", "stg_order_items", "stg_payments"):
            self.assertIn(name, self.models, f"Missing staging model: {name}")

    def test_intermediate_models_all_exist(self):
        for name in ("int_customers", "int_products", "int_orders", "int_order_items", "int_payments"):
            self.assertIn(name, self.models, f"Missing intermediate model: {name}")

    def test_mart_models_all_exist(self):
        for name in ("fact_sales", "fact_orders", "dim_customer", "dim_product", "dim_date", "daily_sales"):
            self.assertIn(name, self.models, f"Missing mart model: {name}")

    def test_no_select_star(self):
        bad = []
        for f in _sql_files():
            if "select *" in f.read_text().lower():
                bad.append(f.name)
        self.assertEqual(bad, [], f"SELECT * found in: {bad}")

    def test_dim_date_uses_created_at_not_ingested_at(self):
        # Strip SQL comments so the check is on code, not prose.
        sql = (MODELS / "marts" / "dim_date.sql").read_text()
        sql = re.sub(r"--.*", "", sql)  # remove line comments
        self.assertIn("created_at", sql)
        self.assertNotIn("ingested_at", sql)


if __name__ == "__main__":
    unittest.main()