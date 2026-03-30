import re

import pytest

from structured_logging.core.context import ServiceContext

@pytest.fixture(autouse=True)
def reset_service_context():
    """Reset ServiceContext singleton state between tests."""
    ServiceContext._service_name = None
    ServiceContext._environment = None
    ServiceContext._initialised = False

def test_initialise_sets_service_identity():
    ServiceContext.initialise("feature-materialiser", "prod")

    assert ServiceContext.service_name() == "feature-materialiser"
    assert ServiceContext.environment() == "prod"

def test_access_before_initialisation_raises():
    with pytest.raises(RuntimeError):
        ServiceContext.service_name()


def test_double_initialisation_raises():
    ServiceContext.initialise("dataset-builder", "staging")

    with pytest.raises(RuntimeError):
        ServiceContext.initialise("inference-gateway", "prod")

def test_empty_values_rejected():
    with pytest.raises(ValueError):
        ServiceContext.initialise("", "prod")

def test_environment_label_is_preserved():
    ServiceContext.initialise("telemetry-ingest", "dev")

    assert ServiceContext.environment() == "dev"

def test_service_name_uses_kebab_case():
    ServiceContext.initialise("trace-aggregator", "prod")

    assert re.fullmatch(r"[a-z0-9]+(-[a-z0-9]+)*", ServiceContext.service_name())