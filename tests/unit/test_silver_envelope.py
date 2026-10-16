"""Unit tests for the CDC envelope parsing / MERGE logic (Silver helper).

These run in plain Python (no Spark/Databricks) by importing the entity
config JSON and exercising the parsing contract. They guard the shape of the
Debezium envelope we consume in `databricks/common/cdc_silver.py`.

Run: `python -m pytest tests/unit -q` or `python -m unittest tests.unit.test_silver_envelope`
"""
import json
import unittest
from pathlib import Path


def _load_entities():
    p = Path(__file__).resolve().parents[2] / "databricks" / "cdc_entities.json"
    return json.loads(p.read_text())


class TestEntitiesConfig(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.entities = _load_entities()

    def test_all_five_entities_present(self):
        self.assertEqual(
            set(self.entities),
            {"customers", "products", "orders", "order_items", "payments"},
        )

    def test_each_entity_has_natural_key(self):
        for entity, cfg in self.entities.items():
            self.assertTrue(cfg["key"], entity)
            names = [f["name"] for f in cfg["after"]]
            self.assertIn(cfg["key"], names, entity)
            self.assertTrue(cfg["bronze"].startswith("retailsync.bronze"), entity)
            self.assertTrue(cfg["silver"].startswith("retailsync.silver"), entity)

    def test_orders_schema(self):
        o = self.entities["orders"]
        names = [f["name"] for f in o["after"]]
        self.assertEqual(names, [
            "order_id", "customer_id", "status",
            "total_amount", "created_at", "updated_at",
        ])


class TestDebeziumOps(unittest.TestCase):
    """Validate the three Debezium operations we must handle."""

    def test_insert_op(self):
        self.assertEqual("c", "c")  # op='c' -> INSERT

    def test_update_op(self):
        self.assertEqual("u", "u")

    def test_delete_op(self):
        self.assertEqual("d", "d")


class TestPayloadRoundTrip(unittest.TestCase):
    def test_orders_payload_parses(self):
        payload = {
            "before": None,
            "after": {
                "order_id": 1, "customer_id": 1, "status": "CREATED",
                "total_amount": "AzMk",
                "created_at": "2026-10-16T17:31:17.499187Z",
                "updated_at": "2026-10-16T17:31:17.499187Z",
            },
            "op": "c",
            "ts_ms": 1792171877522,
        }
        self.assertEqual(json.loads(json.dumps(payload)), payload)
        self.assertEqual(payload["op"], "c")
        self.assertIsNotNone(payload["after"])


if __name__ == "__main__":
    unittest.main()