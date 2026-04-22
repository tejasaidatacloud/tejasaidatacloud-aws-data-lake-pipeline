"""
Data Validation Module
=======================
Reusable validation utilities shared between Lambda and Glue jobs.
Implements Great-Expectations-style checks without the full GE overhead.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Callable

logger = logging.getLogger(__name__)


@dataclass
class CheckResult:
    name:    str
    passed:  bool
    detail:  str = ""
    value:   Any = None


@dataclass
class ValidationSuite:
    suite_name: str
    results:    list[CheckResult] = field(default_factory=list)

    def add(self, result: CheckResult) -> None:
        self.results.append(result)
        icon = "✅" if result.passed else "❌"
        logger.info("%s  [%s] %s: %s", icon, self.suite_name, result.name, result.detail or ("PASS" if result.passed else "FAIL"))

    @property
    def passed(self) -> int:
        return sum(1 for r in self.results if r.passed)

    @property
    def failed(self) -> int:
        return sum(1 for r in self.results if not r.passed)

    @property
    def success_rate(self) -> float:
        total = len(self.results)
        return (self.passed / total * 100) if total > 0 else 0.0

    def summary(self) -> dict:
        return {
            "suite":        self.suite_name,
            "total_checks": len(self.results),
            "passed":       self.passed,
            "failed":       self.failed,
            "success_rate": f"{self.success_rate:.1f}%",
            "overall":      "PASS" if self.failed == 0 else "FAIL",
            "details":      [{"name": r.name, "status": "PASS" if r.passed else "FAIL", "detail": r.detail} for r in self.results],
        }


# ─── Primitive Checks ─────────────────────────────────────────────────────────

def check_not_empty(data: list, name: str = "not_empty") -> CheckResult:
    ok = len(data) > 0
    return CheckResult(name=name, passed=ok, detail=f"{len(data)} records", value=len(data))


def check_min_count(data: list, min_count: int, name: str = "min_count") -> CheckResult:
    ok = len(data) >= min_count
    return CheckResult(name=name, passed=ok, detail=f"{len(data)} >= {min_count}", value=len(data))


def check_required_fields(record: dict, fields: list[str], name: str = "required_fields") -> CheckResult:
    missing = [f for f in fields if f not in record]
    ok = len(missing) == 0
    return CheckResult(
        name=name, passed=ok,
        detail=f"All {len(fields)} fields present" if ok else f"Missing: {missing}",
    )


def check_null_rate(data: list[dict], field_name: str, max_null_rate: float = 0.05) -> CheckResult:
    if not data:
        return CheckResult(name=f"null_rate_{field_name}", passed=False, detail="Empty dataset")
    null_count = sum(1 for r in data if r.get(field_name) is None or r.get(field_name) == "")
    rate = null_count / len(data)
    ok   = rate <= max_null_rate
    return CheckResult(
        name=f"null_rate_{field_name}", passed=ok,
        detail=f"{rate:.1%} ({null_count}/{len(data)}) — threshold {max_null_rate:.0%}",
        value=rate,
    )


def check_value_range(data: list[dict], field_name: str, min_val: float, max_val: float) -> CheckResult:
    out_of_range = [
        r[field_name] for r in data
        if r.get(field_name) is not None and not (min_val <= r[field_name] <= max_val)
    ]
    ok = len(out_of_range) == 0
    return CheckResult(
        name=f"range_{field_name}",
        passed=ok,
        detail=f"All in [{min_val}, {max_val}]" if ok else f"{len(out_of_range)} out-of-range values found",
    )


def check_uniqueness(data: list[dict], key_fields: list[str]) -> CheckResult:
    seen    = set()
    dupes   = 0
    for record in data:
        key = tuple(record.get(f) for f in key_fields)
        if key in seen:
            dupes += 1
        seen.add(key)
    ok = dupes == 0
    return CheckResult(
        name=f"uniqueness_({'_'.join(key_fields)})",
        passed=ok,
        detail=f"No duplicates" if ok else f"{dupes} duplicate key(s) found",
    )


def check_referential_integrity(
    child_data: list[dict],
    child_field: str,
    parent_values: set,
    name: str = "referential_integrity",
) -> CheckResult:
    orphans = [r[child_field] for r in child_data if r.get(child_field) not in parent_values]
    ok = len(orphans) == 0
    return CheckResult(
        name=name,
        passed=ok,
        detail=f"All {len(child_data)} records have valid references" if ok else f"{len(orphans)} orphan records",
    )


def check_custom(name: str, fn: Callable[[], bool], detail: str = "") -> CheckResult:
    """Wrap a custom boolean check."""
    try:
        ok = fn()
        return CheckResult(name=name, passed=ok, detail=detail or ("PASS" if ok else "FAIL"))
    except Exception as exc:
        return CheckResult(name=name, passed=False, detail=f"Exception: {exc}")


# ─── Domain-Specific Suites ───────────────────────────────────────────────────

def validate_weather_records(records: list[dict]) -> ValidationSuite:
    suite = ValidationSuite("weather_validation")
    required = ["date", "hour", "temperature_c", "precipitation_mm", "windspeed_kmh"]

    suite.add(check_not_empty(records))
    suite.add(check_min_count(records, min_count=24))

    if records:
        suite.add(check_required_fields(records[0], required))
        suite.add(check_null_rate(records, "temperature_c"))
        suite.add(check_null_rate(records, "precipitation_mm"))
        suite.add(check_value_range(records, "temperature_c", -89, 57))
        suite.add(check_value_range(records, "hour", 0, 23))
        suite.add(check_value_range(records, "precipitation_mm", 0, 2000))
        suite.add(check_uniqueness(records, ["date", "hour"]))

    return suite


def validate_crypto_records(records: list[dict]) -> ValidationSuite:
    suite = ValidationSuite("crypto_validation")
    required = ["date", "currency", "rate_usd", "rate_gbp", "rate_eur"]

    suite.add(check_not_empty(records))
    suite.add(check_min_count(records, min_count=1))

    if records:
        suite.add(check_required_fields(records[0], required))
        suite.add(check_null_rate(records, "rate_usd", max_null_rate=0.0))  # USD must not be null
        suite.add(check_value_range(records, "rate_usd", 1, 10_000_000))    # sane BTC range
        suite.add(check_value_range(records, "rate_gbp", 1, 10_000_000))
        suite.add(check_uniqueness(records, ["date", "currency"]))

    return suite


def validate_daily_summary(records: list[dict]) -> ValidationSuite:
    suite = ValidationSuite("daily_summary_validation")
    required = ["date", "avg_temp_c", "max_temp_c", "min_temp_c", "total_precip_mm", "btc_usd_avg"]

    suite.add(check_not_empty(records))
    if records:
        suite.add(check_required_fields(records[0], required))
        suite.add(check_null_rate(records, "avg_temp_c"))
        suite.add(check_custom(
            "max_temp_gte_min_temp",
            lambda: all(r.get("max_temp_c", 0) >= r.get("min_temp_c", 0) for r in records),
            "max_temp_c >= min_temp_c for all records",
        ))

    return suite
