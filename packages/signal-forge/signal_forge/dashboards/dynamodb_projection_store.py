"""
signal_forge.dashboards.dynamodb_projection_store

DynamoDB-backed ``ProjectionStore`` — the production backend behind the
projection-store protocol (DDIA Chapter 3: a low-latency key-value store
serving materialised views to a sub-5-second dashboard).

The protocol (commit 3) was designed so the projection folds never touch
AWS; this module is where that design earns its keep. ``InMemoryProjectionStore``
and ``DynamoDbProjectionStore`` are interchangeable implementations of the
same four methods, so a projection tested against the in-memory store behaves
identically against DynamoDB. boto3 lives here and only here — the in-memory
store, which shares the package, imports without boto3 present.

Table schema
------------
``view`` is the partition key, ``key`` is the sort key, the serialised value
is a ``value`` string attribute. This makes ``keys(view)`` a ``Query`` on one
partition (bounded, ordered) rather than a full-table ``Scan`` — the read
pattern ``ActiveOutageProjection.active_outage_count()`` and the anomaly-rate
reads depend on (D-19's hot-path concern). A composite ``"view|key"`` single
partition key was rejected for exactly that reason: it would force ``keys``
to ``Scan``.

The store is a dumb key-value persister: it neither parses nor interprets the
serialised values, mirroring ``InMemoryProjectionStore``. The one structured
thing it does is TTL, and even that is injected (below), not baked in.

TTL: anchored on event-time embedded in the key
-----------------------------------------------
The current-state views (``offline_count``, ``active_outage``) empty
themselves on recovery and must never expire. The anomaly-rate view's buckets
age out instead, and that is what TTL is for (the eviction-(i) decision and
its known-issues note). DynamoDB TTL expires items on a numeric epoch-seconds
attribute.

The expiry must not come from a wall-clock read in the store — that would put
nondeterminism in the persistence layer. Instead it is anchored on the
event-time already embedded in the key: an ``anomaly_rate`` key is
``"signal|bucket_iso"``, so the bucket's own event-time is the anchor and
``expiry = bucket_time + retention``. This keeps the TTL replay-deterministic
and tied to event-time, consistent with the rest of the projection layer.

The store itself stays generic: it holds a ``TtlResolver`` —
``(view, key) -> epoch_expiry | None`` — and writes the ``ttl`` attribute only
when the resolver returns a value. The default resolver understands the
anomaly-rate key shape and leaves every other view untouched; pass an explicit
no-op resolver to disable TTL entirely.

Configuration and the no-op store
----------------------------------
The target table is read from ``PlatformSettings.projection_table`` (D-12:
settings carry the table *name*, a string; the boto3 client is built here from
it). ``for_replay()`` swaps the active table to the replay-isolated one before
the store is constructed (commit 9), so the store is replay-oblivious. When
``projection_table`` is ``None`` — including a replay run whose replay table
was never configured — the store builds no client and every operation is a
no-op (``get`` returns ``None``, ``keys`` returns ``[]``, ``put``/``delete`` do
nothing), the same safety posture the dataset writer takes for an unset bucket.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import datetime
from typing import Any, Final

from signal_forge.config.platform_settings import PlatformSettings
from signal_forge.dashboards.anomaly_rate_projection import AnomalyRateProjection

#: ``(view, key) -> epoch-seconds expiry``, or ``None`` for no TTL on that item.
TtlResolver = Callable[[str, str], int | None]

#: DynamoDB client factory. Untyped under our mypy config; the alias documents
#: the seam where tests inject a moto-backed client.
DynamoClientFactory = Callable[[], Any]

#: The table's TTL attribute (configured on the table via update_time_to_live).
_TTL_ATTRIBUTE: Final[str] = "ttl"

#: Default anomaly-rate bucket retention: 24h, comfortably longer than any
#: plausible dashboard rolling window, short enough to bound bucket storage.
_DEFAULT_ANOMALY_RATE_TTL_SECONDS: Final[int] = 86_400


def _no_ttl(view: str, key: str) -> int | None:
    return None


def make_ttl_resolver(policies: Mapping[str, int]) -> TtlResolver:
    """
    Build a ``TtlResolver`` from a ``{view: retention_seconds}`` policy.

    A view in the policy expires its items at ``bucket_time + retention``,
    where ``bucket_time`` is parsed from the key's trailing ISO timestamp
    (the ``"signal|bucket_iso"`` shape ``AnomalyRateProjection`` writes). Views
    absent from the policy get no TTL. Only the anomaly-rate key shape is
    understood; a future TTL'd view with a different key layout would extend
    this builder.
    """

    def resolve(view: str, key: str) -> int | None:
        retention = policies.get(view)
        if retention is None:
            return None
        _, _, bucket_iso = key.rpartition("|")
        bucket_time = datetime.fromisoformat(bucket_iso)
        return int(bucket_time.timestamp()) + retention

    return resolve


def default_ttl_resolver(
    anomaly_rate_ttl_seconds: int = _DEFAULT_ANOMALY_RATE_TTL_SECONDS,
) -> TtlResolver:
    """The production default: TTL the anomaly-rate view, nothing else."""
    return make_ttl_resolver({AnomalyRateProjection.VIEW: anomaly_rate_ttl_seconds})


def _default_dynamodb_client() -> Any:
    # Imported lazily so this module — and InMemoryProjectionStore, which
    # shares the package — imports without boto3 present.
    import boto3

    return boto3.client("dynamodb")


class DynamoDbProjectionStore:
    """
    DynamoDB-backed ``ProjectionStore``.

    Construct with platform settings and, optionally, a client factory and a
    TTL resolver. The factory seam lets tests inject a moto-backed client;
    production omits it and gets a real boto3 client built from the table
    name. The resolver defaults to the production policy (anomaly-rate TTL);
    pass an explicit no-op resolver to disable TTL. The store assumes the
    table already exists with ``view``/``key`` partition/sort keys and TTL
    configured on the ``ttl`` attribute (provisioned upstream).
    """

    def __init__(
        self,
        *,
        settings: PlatformSettings,
        client_factory: DynamoClientFactory | None = None,
        ttl_resolver: TtlResolver | None = None,
    ) -> None:
        self._table = settings.projection_table
        self._ttl_resolver = (
            ttl_resolver if ttl_resolver is not None else default_ttl_resolver()
        )

        self._client: Any
        if self._table is None:
            # No projection table configured: no client, every op no-ops.
            self._client = None
        else:
            factory = client_factory or _default_dynamodb_client
            self._client = factory()

    def get(self, view: str, key: str) -> str | None:
        if self._table is None:
            return None
        response = self._client.get_item(
            TableName=self._table,
            Key={"view": {"S": view}, "key": {"S": key}},
        )
        item = response.get("Item")
        if item is None:
            return None
        # str() narrows the boto3 Any to the declared return type; the
        # attribute is a DynamoDB string ("S"), so this is a no-op coercion.
        return str(item["value"]["S"])

    def put(self, view: str, key: str, value: str) -> None:
        if self._table is None:
            return
        item: dict[str, Any] = {
            "view": {"S": view},
            "key": {"S": key},
            "value": {"S": value},
        }
        expiry = self._ttl_resolver(view, key)
        if expiry is not None:
            item[_TTL_ATTRIBUTE] = {"N": str(expiry)}
        self._client.put_item(TableName=self._table, Item=item)

    def delete(self, view: str, key: str) -> None:
        if self._table is None:
            return
        self._client.delete_item(
            TableName=self._table,
            Key={"view": {"S": view}, "key": {"S": key}},
        )

    def keys(self, view: str) -> list[str]:
        if self._table is None:
            return []
        # ``view`` and ``key`` are DynamoDB reserved words, so alias both via
        # ExpressionAttributeNames. Query the one partition and paginate.
        collected: list[str] = []
        start_key: dict[str, Any] | None = None
        while True:
            kwargs: dict[str, Any] = {
                "TableName": self._table,
                "KeyConditionExpression": "#v = :view",
                "ProjectionExpression": "#k",
                "ExpressionAttributeNames": {"#v": "view", "#k": "key"},
                "ExpressionAttributeValues": {":view": {"S": view}},
            }
            if start_key is not None:
                kwargs["ExclusiveStartKey"] = start_key
            response = self._client.query(**kwargs)
            collected.extend(item["key"]["S"] for item in response.get("Items", []))
            start_key = response.get("LastEvaluatedKey")
            if start_key is None:
                break
        return sorted(collected)
