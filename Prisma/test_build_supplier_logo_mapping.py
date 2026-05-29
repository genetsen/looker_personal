"""Tests for the supplier-logo mapping review utility.

These tests cover the deterministic pieces of the script so review CSV output
can be validated without calling BigQuery or the public internet.
"""

from __future__ import annotations

import csv
import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT_PATH = Path(__file__).with_name("build_supplier_logo_mapping.py")
SPEC = importlib.util.spec_from_file_location("build_supplier_logo_mapping", SCRIPT_PATH)
logo_mapping = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = logo_mapping
SPEC.loader.exec_module(logo_mapping)


class SupplierLogoMappingTests(unittest.TestCase):
    def test_alias_resolution_builds_hunter_logo_url(self) -> None:
        row = logo_mapping.resolve_supplier("google_ads")

        self.assertEqual(row.resolved_domain, "google.com")
        self.assertEqual(row.logo_url_final, "https://logos.hunter.io/google.com")
        self.assertEqual(row.resolution_method, "alias")
        self.assertEqual(row.needs_review, "false")

    def test_search_resolution_uses_first_result_domain(self) -> None:
        def fake_search(_: str) -> str | None:
            return "https://www.smartbrief.com/original-page?x=1"

        row = logo_mapping.resolve_supplier(
            "Unknown Example", search_domain=fake_search
        )

        self.assertEqual(row.resolved_domain, "smartbrief.com")
        self.assertEqual(row.logo_url_final, "https://logos.hunter.io/smartbrief.com")
        self.assertEqual(row.resolution_method, "search_first_result")
        self.assertEqual(row.confidence, "medium")

    def test_search_error_falls_back_to_domain_guess(self) -> None:
        def failing_search(_: str) -> str | None:
            raise OSError("search unavailable")

        row = logo_mapping.resolve_supplier("Unknown Example", search_domain=failing_search)

        self.assertEqual(row.resolved_domain, "unknownexample.com")
        self.assertEqual(row.resolution_method, "domain_guess")
        self.assertEqual(row.needs_review, "true")

    def test_write_review_csv_keeps_one_row_per_supplier(self) -> None:
        suppliers = [
            logo_mapping.SupplierStats("meta", 10, 10),
            logo_mapping.SupplierStats("Unknown Example", 2, 2),
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "review.csv"
            logo_mapping.write_review_csv(
                suppliers, output_path, search_domain=lambda _: None
            )

            with output_path.open(newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))

        self.assertEqual(
            [row["supplier_name"] for row in rows], ["meta", "Unknown Example"]
        )
        self.assertEqual(rows[0]["logo_url_final"], "https://logos.hunter.io/meta.com")
        self.assertEqual(rows[1]["needs_review"], "true")


if __name__ == "__main__":
    unittest.main()
