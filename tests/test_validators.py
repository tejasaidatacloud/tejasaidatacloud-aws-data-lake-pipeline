"""
Unit Tests – Data Validation Module
=====================================
Run with: pytest tests/ -v --tb=short
"""

import pytest
from data_validation.validators import (
    check_not_empty,
    check_min_count,
    check_required_fields,
    check_null_rate,
    check_value_range,
    check_uniqueness,
    check_custom,
    validate_weather_records,
    validate_crypto_records,
    validate_daily_summary,
    ValidationSuite,
    CheckResult,
)


# ─── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture
def sample_weather_records():
    return [
        {"date": "2026-04-22", "hour": h, "temperature_c": 15.0 + h * 0.3,
         "precipitation_mm": 0.0, "windspeed_kmh": 12.5, "source": "open_meteo",
         "ingested_at": "2026-04-22T02:00:00Z", "execution_date": "2026-04-22"}
        for h in range(24)
    ]


@pytest.fixture
def sample_crypto_records():
    return [
        {"date": "2026-04-22", "currency": "BTC", "rate_usd": 65000.0,
         "rate_gbp": 51800.0, "rate_eur": 61400.0, "description": "Bitcoin",
         "ingested_at": "2026-04-22T02:00:00Z", "execution_date": "2026-04-22"}
    ]


@pytest.fixture
def sample_daily_summary():
    return [
        {"date": "2026-04-22", "avg_temp_c": 18.5, "max_temp_c": 22.1,
         "min_temp_c": 14.2, "total_precip_mm": 0.0, "btc_usd_avg": 65000.0,
         "processed_at": "2026-04-22T03:00:00Z"}
    ]


# ─── Primitive Check Tests ─────────────────────────────────────────────────────

class TestPrimitiveChecks:

    def test_not_empty_passes_with_data(self):
        result = check_not_empty([1, 2, 3])
        assert result.passed is True
        assert result.value == 3

    def test_not_empty_fails_with_empty_list(self):
        result = check_not_empty([])
        assert result.passed is False

    def test_min_count_passes_when_above_threshold(self):
        data = list(range(100))
        result = check_min_count(data, min_count=50)
        assert result.passed is True

    def test_min_count_fails_when_below_threshold(self):
        data = list(range(5))
        result = check_min_count(data, min_count=10)
        assert result.passed is False

    def test_required_fields_passes_with_all_fields(self):
        record = {"name": "Alice", "age": 30, "email": "alice@example.com"}
        result = check_required_fields(record, ["name", "age", "email"])
        assert result.passed is True

    def test_required_fields_fails_with_missing_field(self):
        record = {"name": "Alice"}
        result = check_required_fields(record, ["name", "age", "email"])
        assert result.passed is False
        assert "age" in result.detail or "email" in result.detail

    def test_null_rate_passes_below_threshold(self):
        data = [{"temp": 10.0}] * 95 + [{"temp": None}] * 5  # 5% null
        result = check_null_rate(data, "temp", max_null_rate=0.05)
        assert result.passed is True

    def test_null_rate_fails_above_threshold(self):
        data = [{"temp": 10.0}] * 80 + [{"temp": None}] * 20  # 20% null
        result = check_null_rate(data, "temp", max_null_rate=0.05)
        assert result.passed is False

    def test_null_rate_empty_dataset(self):
        result = check_null_rate([], "temp")
        assert result.passed is False

    def test_value_range_passes_all_in_range(self):
        data = [{"temp": t} for t in [10, 20, 30, 15]]
        result = check_value_range(data, "temp", 0, 40)
        assert result.passed is True

    def test_value_range_fails_with_outlier(self):
        data = [{"temp": t} for t in [10, 200, 30]]  # 200 is out of range
        result = check_value_range(data, "temp", -89, 57)
        assert result.passed is False

    def test_uniqueness_passes_no_duplicates(self):
        data = [{"date": "2026-04-22", "hour": h} for h in range(24)]
        result = check_uniqueness(data, ["date", "hour"])
        assert result.passed is True

    def test_uniqueness_fails_with_duplicates(self):
        data = [{"date": "2026-04-22", "hour": 0}] * 3
        result = check_uniqueness(data, ["date", "hour"])
        assert result.passed is False

    def test_custom_check_passes(self):
        result = check_custom("always_true", lambda: True, "should pass")
        assert result.passed is True

    def test_custom_check_fails(self):
        result = check_custom("always_false", lambda: False, "should fail")
        assert result.passed is False

    def test_custom_check_handles_exception(self):
        result = check_custom("raises", lambda: 1 / 0, "division by zero")
        assert result.passed is False
        assert "Exception" in result.detail


# ─── ValidationSuite Tests ────────────────────────────────────────────────────

class TestValidationSuite:

    def test_suite_tracks_pass_fail_counts(self):
        suite = ValidationSuite("test_suite")
        suite.add(CheckResult("check1", True))
        suite.add(CheckResult("check2", False))
        suite.add(CheckResult("check3", True))
        assert suite.passed == 2
        assert suite.failed == 1

    def test_suite_success_rate(self):
        suite = ValidationSuite("test_suite")
        for i in range(8):
            suite.add(CheckResult(f"check_{i}", True))
        for i in range(2):
            suite.add(CheckResult(f"fail_{i}", False))
        assert suite.success_rate == pytest.approx(80.0)

    def test_suite_overall_pass_when_no_failures(self):
        suite = ValidationSuite("test_suite")
        suite.add(CheckResult("check1", True))
        assert suite.overall == "PASS"

    def test_suite_overall_fail_with_any_failure(self):
        suite = ValidationSuite("test_suite")
        suite.add(CheckResult("check1", True))
        suite.add(CheckResult("check2", False))
        assert suite.overall == "FAIL"

    def test_suite_summary_structure(self):
        suite = ValidationSuite("weather_validation")
        suite.add(CheckResult("not_empty", True, "24 records"))
        summary = suite.summary()
        assert "suite" in summary
        assert "total_checks" in summary
        assert "passed" in summary
        assert "failed" in summary
        assert "overall" in summary
        assert "details" in summary

    def test_empty_suite_has_zero_rate(self):
        suite = ValidationSuite("empty")
        assert suite.success_rate == 0.0


# ─── Domain Validator Tests ───────────────────────────────────────────────────

class TestWeatherValidation:

    def test_passes_with_valid_records(self, sample_weather_records):
        suite = validate_weather_records(sample_weather_records)
        assert suite.passed >= 5
        assert suite.failed == 0

    def test_fails_with_empty_records(self):
        suite = validate_weather_records([])
        assert suite.failed >= 1
        assert suite.overall == "FAIL"

    def test_fails_with_out_of_range_temperature(self, sample_weather_records):
        bad = sample_weather_records.copy()
        bad[0]["temperature_c"] = 200.0  # way out of range
        suite = validate_weather_records(bad)
        assert suite.overall == "FAIL"

    def test_fails_with_duplicate_hour(self):
        records = [
            {"date": "2026-04-22", "hour": 0, "temperature_c": 10.0,
             "precipitation_mm": 0.0, "windspeed_kmh": 5.0}
        ] * 2  # duplicate hour
        suite = validate_weather_records(records)
        assert suite.overall == "FAIL"

    def test_fails_with_missing_required_field(self, sample_weather_records):
        bad = [dict(r) for r in sample_weather_records]
        for r in bad:
            del r["temperature_c"]
        suite = validate_weather_records(bad)
        assert suite.overall == "FAIL"

    def test_fails_with_high_null_rate(self, sample_weather_records):
        bad = [dict(r) for r in sample_weather_records]
        for r in bad[::2]:
            r["temperature_c"] = None  # ~50% null
        suite = validate_weather_records(bad)
        assert suite.overall == "FAIL"


class TestCryptoValidation:

    def test_passes_with_valid_record(self, sample_crypto_records):
        suite = validate_crypto_records(sample_crypto_records)
        assert suite.passed >= 4
        assert suite.overall == "PASS"

    def test_fails_with_zero_rate(self, sample_crypto_records):
        bad = [dict(r) for r in sample_crypto_records]
        bad[0]["rate_usd"] = 0.0
        suite = validate_crypto_records(bad)
        assert suite.overall == "FAIL"

    def test_fails_with_null_usd_rate(self, sample_crypto_records):
        bad = [dict(r) for r in sample_crypto_records]
        bad[0]["rate_usd"] = None
        suite = validate_crypto_records(bad)
        assert suite.overall == "FAIL"


class TestDailySummaryValidation:

    def test_passes_with_valid_summary(self, sample_daily_summary):
        suite = validate_daily_summary(sample_daily_summary)
        assert suite.overall == "PASS"

    def test_fails_when_max_less_than_min_temp(self, sample_daily_summary):
        bad = [dict(r) for r in sample_daily_summary]
        bad[0]["max_temp_c"] = 10.0
        bad[0]["min_temp_c"] = 20.0  # min > max – invalid
        suite = validate_daily_summary(bad)
        assert suite.overall == "FAIL"

    def test_fails_with_empty_summary(self):
        suite = validate_daily_summary([])
        assert suite.overall == "FAIL"
