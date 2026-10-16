"""Contract test for the Silver MERGE upsert logic (no Spark).

Simulates the idempotent MERGE behavior defined in
`databricks/common/cdc_silver.py` using plain Python dicts, guarding:
  - INSERT / UPDATE / DELETE (`op` = c / u / d) on the natural key
  - Idempotency (re-processing the same event is a no-op)
  - Delete-then-reinsert resurrection
  - All 5 entities from `databricks/cdc_entities.json`

Run: `python -m pytest tests/unit -q`
"""
import json
import unittest
from pathlib import Path


def _load_entities():
    p = Path(__file__).resolve().parents[2] / "databricks" / "cdc_entities.json"
    return json.loads(p.read_text())


def _envelope(after, before=None, op="c", ts_ms=1):
    return {"before": before, "after": after, "op": op, "ts_ms": ts_ms}


def merge_row(state, key_col, event):
    """Simulate one Silver MERGE row. Returns the new state dict."""
    envelope = event
    after = envelope.get("after")
    before = envelope.get("before")
    op = envelope.get("op")

    pk = (after or before or {}).get(key_col)

    if op == "d":
        # DELETE: remove the row if present.
        state.pop(pk, None)
        return state

    if op in ("c", "u"):
        # UPSERT: overwrite by natural key (idempotent).
        if after is not None:
            state[pk] = after
        return state

    raise ValueError(f"unknown op {op!r}")


class TestSilverMergeContract(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.entities = _load_entities()

    def _fresh_state(self, entity):
        return {}

    def test_insert_then_update_then_delete_all_entities(self):
        for entity, cfg in self.entities.items():
            key = cfg["key"]
            state = self._fresh_state(entity)

            after_insert = {key: 100, "status": "CREATED"}
            state = merge_row(state, key, _envelope(after_insert, op="c"))
            self.assertEqual(state[100]["status"], "CREATED", entity)

            after_update = {key: 100, "status": "PAID"}
            state = merge_row(state, key, _envelope(after_update, op="u"))
            self.assertEqual(state[100]["status"], "PAID", entity)

            state = merge_row(state, key, _envelope(None, before={key: 100}, op="d"))
            self.assertNotIn(100, state, entity)

    def test_idempotent_reprocess_insert(self):
        for entity, cfg in self.entities.items():
            key = cfg["key"]
            state = self._fresh_state(entity)
            after = {key: 42, "status": "CREATED"}
            state = merge_row(state, key, _envelope(after, op="c"))
            state = merge_row(state, key, _envelope(after, op="c"))
            self.assertEqual(len(state), 1, entity)
            self.assertEqual(state[42]["status"], "CREATED", entity)

    def test_delete_nonexistent_is_noop(self):
        for entity, cfg in self.entities.items():
            key = cfg["key"]
            state = self._fresh_state(entity)
            state = merge_row(state, key, _envelope(None, before={key: 999}, op="d"))
            self.assertEqual(len(state), 0, entity)

    def test_resurrect_after_delete(self):
        for entity, cfg in self.entities.items():
            key = cfg["key"]
            state = self._fresh_state(entity)
            after = {key: 7, "status": "CREATED"}
            state = merge_row(state, key, _envelope(after, op="c"))
            state = merge_row(state, key, _envelope(None, before={key: 7}, op="d"))
            self.assertNotIn(7, state, entity)
            state = merge_row(state, key, _envelope(after, op="c"))
            self.assertEqual(state[7]["status"], "CREATED", entity)

    def test_natural_key_is_in_after_schema(self):
        for entity, cfg in self.entities.items():
            names = [f["name"] for f in cfg["after"]]
            self.assertIn(cfg["key"], names, entity)


if __name__ == "__main__":
    unittest.main()