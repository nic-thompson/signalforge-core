"""
signal_forge.features

Feature-emission constants and helpers.

The streaming pipeline (``signal_forge.streaming.realtime_pipeline``)
bundles ``WindowEmission``s into ``WindowedFeatureVectorEvent``s per
``(partition_key, window_start)`` group. The bundled events use
``FEATURE_SCHEMA_VERSION`` as their ``feature_version`` field.

The version is a property of the *feature set* — which named features
the pipeline produces and what they mean. Versioning happens through
code changes to which aggregators get registered; the constant string
moves in lockstep. Stays at ``"v1"`` until a backwards-incompatible
change to the produced feature set is made (e.g. renaming a feature
or changing its semantics for downstream consumers).
"""

from __future__ import annotations

from typing import Final

FEATURE_SCHEMA_VERSION: Final[str] = "v1"
