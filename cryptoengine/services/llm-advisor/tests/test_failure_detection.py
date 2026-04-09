"""Unit tests for FailureDetector validation framework."""

import pytest
import sys
sys.path.insert(0, ".")

from data_sources.failure_detection import FailureDetector
from data_sources.base import SourceHealth


class TestCheckHttp:
    def setup_method(self):
        self.detector = FailureDetector(source_name="test")

    def test_200_healthy(self):
        h = self.detector.check_http(200, content_length=1000)
        assert h.status == "HEALTHY"

    def test_500_broken(self):
        h = self.detector.check_http(500)
        assert h.status == "BROKEN"
        assert "500" in h.failure_reason

    def test_403_broken(self):
        h = self.detector.check_http(403)
        assert h.status == "BROKEN"

    def test_404_broken(self):
        h = self.detector.check_http(404)
        assert h.status == "BROKEN"
        assert "404" in h.failure_reason

    def test_301_degraded(self):
        h = self.detector.check_http(301)
        assert h.status == "DEGRADED"

    def test_empty_body_broken(self):
        h = self.detector.check_http(200, content_length=0)
        assert h.status == "BROKEN"


class TestCheckJsonStructure:
    def setup_method(self):
        self.detector = FailureDetector(source_name="test")

    def test_none_broken(self):
        h = self.detector.check_json_structure(None, required_keys={"a"})
        assert h.status == "BROKEN"

    def test_empty_list_broken(self):
        h = self.detector.check_json_structure([], required_keys=None)
        assert h.status == "BROKEN"

    def test_all_keys_present(self):
        h = self.detector.check_json_structure({"a": 1, "b": 2}, required_keys={"a", "b"})
        assert h.status == "HEALTHY"

    def test_partial_keys_degraded(self):
        h = self.detector.check_json_structure({"a": 1}, required_keys={"a", "b", "c"})
        assert h.status == "DEGRADED"
        assert "b" in h.fields_missing or "c" in h.fields_missing

    def test_all_keys_missing_broken(self):
        h = self.detector.check_json_structure({"x": 1}, required_keys={"a", "b"})
        assert h.status == "BROKEN"


class TestCheckValueSanity:
    def setup_method(self):
        self.detector = FailureDetector(source_name="test")

    def test_in_range_healthy(self):
        h = self.detector.check_value_sanity({"btc_price": 90000}, {"btc_price": (1000, 1000000)})
        assert h.status == "HEALTHY"

    def test_out_of_range_degraded(self):
        h = self.detector.check_value_sanity({"btc_price": 10}, {"btc_price": (1000, 1000000)})
        assert h.status == "DEGRADED"
        assert "btc_price" in h.failure_reason

    def test_missing_field_skipped(self):
        h = self.detector.check_value_sanity({}, {"btc_price": (1000, 1000000)})
        assert h.status == "HEALTHY"


class TestCombine:
    def setup_method(self):
        self.detector = FailureDetector(source_name="test")

    def test_all_healthy(self):
        h1 = SourceHealth(status="HEALTHY")
        h2 = SourceHealth(status="HEALTHY")
        combined = self.detector.combine(h1, h2)
        assert combined.status == "HEALTHY"

    def test_worst_wins(self):
        h1 = SourceHealth(status="HEALTHY")
        h2 = SourceHealth(status="DEGRADED", failure_reason="partial")
        h3 = SourceHealth(status="BROKEN", failure_reason="down")
        combined = self.detector.combine(h1, h2, h3)
        assert combined.status == "BROKEN"

    def test_degraded_beats_healthy(self):
        h1 = SourceHealth(status="HEALTHY")
        h2 = SourceHealth(status="DEGRADED", failure_reason="missing fields", failure_stage="schema")
        combined = self.detector.combine(h1, h2)
        assert combined.status == "DEGRADED"

    def test_reasons_merged(self):
        h1 = SourceHealth(status="DEGRADED", failure_reason="partial", failure_stage="schema")
        h2 = SourceHealth(status="DEGRADED", failure_reason="out of range", failure_stage="sanity")
        combined = self.detector.combine(h1, h2)
        assert "partial" in combined.failure_reason
        assert "out of range" in combined.failure_reason
