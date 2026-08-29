"""
Tests for the UUIDv5 derivation.

These are pinning tests more than behavioural ones. The namespace, the
separator and every role string are frozen: changing any of them silently
re-bases every derived id in the system, which would make a replay
diverge from the run it is meant to reproduce, and would split one
physical device into two identities across a version boundary. Nothing
would error. The assertions below are what turn that from a silent
failure into a failing build.

The literal expected UUIDs are deliberate. Asserting that
``derive_device_id`` equals ``uuid5(NAMESPACE, "device|...")`` would pass
even if the namespace changed, because both sides would move together. A
hardcoded value is the only assertion that catches that.
"""

from uuid import NAMESPACE_DNS, UUID, uuid5

import pytest

from event_schema_contracts.base.identity import (
    NAMESPACE,
    derive,
    derive_device_id,
)


# ---------------------------------------------------------------
# Frozen constants
# ---------------------------------------------------------------


def test_namespace_is_dns_derived():
    """
    The namespace is derived rather than minted, so it is reproducible
    from nothing and can be re-checked by anyone.
    """

    assert NAMESPACE == uuid5(NAMESPACE_DNS, "signalforge.analytics")


def test_namespace_literal_value_is_pinned():
    """
    Catches a change to the derivation input, which the test above
    cannot: it would move with the constant.
    """

    assert str(NAMESPACE) == "f600ea3b-e95a-5ef4-8e1f-467cf395dd14"


def test_derived_id_literal_value_is_pinned():
    """
    The end-to-end pin. This value is what a device in store-1 labelled
    headset-0001 has been and must remain.
    """

    assert str(derive_device_id("store-1", "headset-0001")) == (
        "9e8407a9-cc81-58a9-8cba-4f9c74918a81"
    )


# ---------------------------------------------------------------
# derive()
# ---------------------------------------------------------------


def test_derive_returns_a_uuid5():
    assert derive("role.x", "store-1").version == 5


def test_same_inputs_are_stable():
    """The property replay depends on."""

    assert derive("role.x", "store-1", 5) == derive("role.x", "store-1", 5)


def test_role_distinguishes_identical_parts():
    """
    Two fields built from the same coordinates — a detection's id and its
    envelope's event_id, say — must not collide. The role keeps them
    apart.
    """

    assert derive("detection", "x") != derive("event", "x")


def test_parts_distinguish_identical_role():
    assert derive("r", "store-1") != derive("r", "store-2")


def test_parts_are_stringified():
    """Non-string coordinates are permitted and stringified."""

    assert derive("r", 5) == derive("r", "5")


def test_separator_cannot_be_forged_across_parts():
    """
    The separator is what keeps ("a", "b") from colliding with ("a|b",).
    A part containing a pipe would break that — which is why every part
    type in use forbids one. This documents the boundary rather than
    claiming it is enforced.
    """

    assert derive("r", "a", "b") != derive("r", "ab")


# ---------------------------------------------------------------
# derive_device_id()
# ---------------------------------------------------------------


def test_device_id_is_derive_with_the_device_role():
    assert derive_device_id("store-1", "headset-12") == derive(
        "device", "store-1", "headset-12"
    )


def test_same_label_in_different_stores_is_a_different_device():
    """
    The reason store_id is part of the identity rather than an attribute
    of it. Two stores may each have a headset-12; they are two devices.
    """

    assert derive_device_id("store-bristol", "headset-12") != derive_device_id(
        "store-leeds", "headset-12"
    )


def test_different_labels_in_one_store_are_different_devices():
    assert derive_device_id("store-1", "headset-12") != derive_device_id(
        "store-1", "headset-13"
    )


def test_device_id_is_stable_across_calls():
    assert derive_device_id("store-1", "headset-12") == derive_device_id(
        "store-1", "headset-12"
    )


def test_device_id_is_a_uuid5():
    """
    SipRegistrationPayload declares device_id under
    __uuid_v4_or_v5_fields__, so a derived id must be v5 to be accepted.
    """

    assert derive_device_id("store-1", "headset-12").version == 5


@pytest.mark.parametrize(
    "store_id,device_label",
    [
        ("store-1", "headset-12"),
        ("s", "d"),
        ("store-with-a-much-longer-name", "device.label_with-punctuation"),
    ],
)
def test_derivation_accepts_any_conforming_pair(store_id, device_label):
    assert isinstance(derive_device_id(store_id, device_label), UUID)
