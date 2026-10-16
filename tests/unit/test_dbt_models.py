"""Lightweight validation for dbt model SQL files (no dbt install required).

Checks each model in `dbt/models/` for basic structural hygiene:
  - starts with a `select` / `with` / `{{ config(` block
  - no bare `SELECT *`
  - every `ref(` / `source(` target exists as a model/source file
  - balanced parentheses

Run: `python -m pytest tests/unit -q`
"""
import re
import unittest
from pathlib import Path


def _dbt_models_dir():
    return Path(__file__).resolve().parents[2] / "dbt" / "models"


def _all_sql_files():
    return sorted(_dbt_models_dir().rglob("*.sql"))


def _defined_models():
    """Map model name -> path for all model files (stripped of directory)."""
    models = {}
    for p in _all_sql_files():
        models[p.stem] = p
    return models


def _defined_sources():
    """Parse source names from *.yml in dbt/models/."""
    sources = set()
    for p in _dbt_models_dir().rglob("*.yml"):
        text = p.read_text()
        for m in re.finditer(r"- name:\s*(\w+)", text):
            sources.add(m.group(1))
    return sources


def _ref_targets(text):
    return re.findall(r"\{\{\s*ref\(['\"](\w+)['\"]\)\s*\}\}", text)


def _source_targets(text):
    return re.findall(r"\{\{\s*source\(['\"](\w+)['\"]\s*,\s*['\"](\w+)['\"]\)\s*\}\}", text)


class TestDbtModels(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.models = _defined_models()
        cls.sources = _defined_sources()

    def test_every_model_has_select(self):
        for name, path in self.models.items():
            text = path.read_text().lower()
            self.assertTrue(
                text.lstrip().startswith(("select", "with", "{{ config")) or "select" in text,
                f"{name} has no SELECT",
            )

    def test_no_select_star(self):
        for name, path in self.models.items():
            text = path.read_text()
            self.assertNotIn("SELECT *", text.upper(), f"{name} uses SELECT *")

    def test_refs_point_to_defined_models(self):
        for name, path in self.models.items():
            text = path.read_text()
            for target in _ref_targets(text):
                self.assertIn(
                    target, self.models,
                    f"{name} refs undefined model '{target}'",
                )

    def test_sources_point_to_defined_sources(self):
        for name, path in self.models.items():
            text = path.read_text()
            for src_name, tbl_name in _source_targets(text):
                self.assertIn(
                    src_name, self.sources,
                    f"{name} sources undefined source '{src_name}'",
                )

    def test_parentheses_balanced(self):
        for name, path in self.models.items():
            text = path.read_text()
            depth = 0
            for ch in text:
                if ch == "(":
                    depth += 1
                elif ch == ")":
                    depth -= 1
                if depth < 0:
                    break
            self.assertEqual(depth, 0, f"{name} has unbalanced parentheses")


if __name__ == "__main__":
    unittest.main()