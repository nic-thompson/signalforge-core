"""
Tests for compatibility.py — parse_version and ensure_compatibility.
"""

import pytest

from event_schema_contracts.versioning.compatibility import (
    SchemaVersion,
    ensure_compatibility,
    parse_version,
)


# ---------------------------------------------------------------------------
# parse_version — valid inputs
# ---------------------------------------------------------------------------

def test_parse_major_only():
    assert parse_version("v1") == SchemaVersion(major=1)


def test_parse_major_minor():
    assert parse_version("v1.2") == SchemaVersion(major=1, minor=2)


def test_parse_major_minor_patch():
    assert parse_version("v1.2.3") == SchemaVersion(major=1, minor=2, patch=3)


def test_parse_zero_major():
    assert parse_version("v0") == SchemaVersion(major=0)


def test_parse_large_components():
    result = parse_version("v10.20.30")
    assert result == SchemaVersion(major=10, minor=20, patch=30)


# ---------------------------------------------------------------------------
# parse_version — invalid inputs
# ---------------------------------------------------------------------------

def test_parse_missing_v_prefix_raises():
    with pytest.raises(ValueError):
        parse_version("1.0.0")


def test_parse_empty_string_raises():
    with pytest.raises(ValueError):
        parse_version("")


def test_parse_bare_v_raises():
    with pytest.raises(ValueError):
        parse_version("v")


def test_parse_too_many_components_raises():
    with pytest.raises(ValueError):
        parse_version("v1.2.3.4")


def test_parse_empty_component_raises():
    with pytest.raises(ValueError):
        parse_version("v1..2")


def test_parse_trailing_dot_raises():
    with pytest.raises(ValueError):
        parse_version("v1.")


def test_parse_leading_zero_raises():
    with pytest.raises(ValueError):
        parse_version("v01.2.3")


def test_parse_non_numeric_component_raises():
    with pytest.raises(ValueError):
        parse_version("v1.alpha.3")


def test_parse_negative_component_raises():
    with pytest.raises(ValueError):
        parse_version("v1.-1.0")


# ---------------------------------------------------------------------------
# SchemaVersion ordering
# ---------------------------------------------------------------------------

def test_version_ordering_major():
    assert parse_version("v2") > parse_version("v1")


def test_version_ordering_minor():
    assert parse_version("v1.2") > parse_version("v1.1")


def test_version_ordering_patch():
    assert parse_version("v1.1.2") > parse_version("v1.1.1")


def test_version_equality():
    assert parse_version("v1.2.3") == parse_version("v1.2.3")


# ---------------------------------------------------------------------------
# ensure_compatibility — allowed upgrades
# ---------------------------------------------------------------------------

def test_same_version_is_compatible():
    ensure_compatibility("v1", "v1")  # must not raise


def test_minor_upgrade_is_compatible():
    ensure_compatibility("v1", "v1.1")  # must not raise


def test_patch_upgrade_is_compatible():
    ensure_compatibility("v1.1", "v1.1.1")  # must not raise


def test_minor_to_higher_minor_is_compatible():
    ensure_compatibility("v1.1", "v1.2")  # must not raise


# ---------------------------------------------------------------------------
# ensure_compatibility — rejected upgrades
# ---------------------------------------------------------------------------

def test_major_version_change_raises():
    with pytest.raises(ValueError, match="Incompatible"):
        ensure_compatibility("v1", "v2")


def test_cross_major_downgrade_raises():
    with pytest.raises(ValueError):
        ensure_compatibility("v2", "v1")


def test_minor_downgrade_within_major_raises():
    with pytest.raises(ValueError, match="Backward upgrade"):
        ensure_compatibility("v1.2", "v1.1")


def test_patch_downgrade_raises():
    with pytest.raises(ValueError, match="Backward upgrade"):
        ensure_compatibility("v1.1.1", "v1.1.0")
