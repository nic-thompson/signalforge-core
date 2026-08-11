# Base model conventions

The `base/` package uses one pattern in three places. Recognising it once saves
reverse-engineering it per module.

## The field-injection pattern

`UUIDv4Model`, `UTCTimestampModel` and `SemVerModel` each enforce a rule on
*selected* fields rather than on all of them. A subclass opts a field in by
naming it in a `ClassVar` tuple:

```python
class DetectionEventPayload(DomainEventPayload):
    __uuid_v4_or_v5_fields__ = ("detection_id", "source_event_id")
    __utc_fields__ = ("detected_at",)
```

The mechanism is a wildcard validator that inspects the field name:

```python
@field_validator("*", check_fields=False)
@classmethod
def validate_utc_fields(cls, value, info):
    if isinstance(value, datetime) and info.field_name in cls.__utc_fields__:
        ...
    return value
```

Registering against `"*"` means the validator runs for every field on the
subclass; the `info.field_name in cls.__..._fields__` check is what narrows it
back down. `check_fields=False` is required because the base class declares no
fields of its own, so pydantic must not verify that `"*"` resolves to anything
at definition time.

The three policies:

| Base | ClassVar | Rule |
|---|---|---|
| `UUIDv4Model` | `__uuid_v4_fields__` | must be UUIDv4 |
| `UUIDv4Model` | `__uuid_v4_or_v5_fields__` | UUIDv4 or UUIDv5 |
| `UTCTimestampModel` | `__utc_fields__` | timezone-aware and UTC |
| `SemVerModel` | `__semver_fields__` | strict `MAJOR.MINOR.PATCH` |

`DomainEventPayload` inherits the first two bases, so any payload has the UUID
and UTC policies available without declaring anything extra.

### Consequences worth knowing

**The tuples are validated, the names are not.** `UUIDv4Model` and
`UTCTimestampModel` both check in `__init_subclass__` that the ClassVar is a
tuple — which catches a bare string, whose characters would otherwise be
iterated one at a time and match nothing. Neither checks that the *names* in
the tuple correspond to real fields. A typo means the field is silently
unvalidated.

**Rules are keyed by name, not by type.** Two fields of the same type on the
same model can be governed differently, so a payload can accept a
deterministic UUIDv5 on one identifier while requiring UUIDv4 on another.

**Nothing is enforced by default.** A field is unconstrained until listed. The
policies are opt-in per field, not opt-out.

## Immutability

Every model sets `frozen=True` and `extra="forbid"`. Events are immutable once
constructed because an event is a record of something that happened and replay
must reproduce it exactly; unknown fields are rejected because at an ingestion
boundary an unrecognised field indicates a producer/consumer contract mismatch
rather than something safe to discard.

## UUID version policy

`__uuid_v4_fields__` is for identifiers that must not be derivable — a
consumer should not be able to guess or reconstruct them. `__uuid_v4_or_v5_fields__`
additionally permits UUIDv5, which is derived deterministically from a
namespace plus a name, so a consumer replaying the same input regenerates an
identical identifier. UUIDv1 and UUIDv3 are rejected on both: v1 encodes host
and time, v3 is an opaque MD5, and neither has a use on these fields.
