from dataclasses import dataclass

@dataclass(frozen=True, order=True)
class SchemaVersion:
    """
    Parsed semantic schema version.

    Supports:

    v1
    v1.1
    v1.1.1
    """

    major: int
    minor: int = 0
    patch: int = 0


def parse_version(version: str) -> SchemaVersion:
    """
    Parse schema version string into structures version object.

    Supports:
    v1
    v1.1
    v1.1.1

    Enforces:
    - numeric components only
    - no empty components
    - max 3 components
    - no leading zeros (except "0")
    """

    if not version.startswith("v"):
        raise ValueError(f"Invalid schema version: {version}")
    
    numeric = version[1:]

    # Explicit empty check (reject "v")
    if not numeric:
        raise ValueError(f"Invalid schema version: {version}")
    
    parts = numeric.split(".")

    # Reject empty components (reject "v.", "v1.", "v1..2")
    if any(p == "" for p in parts):
        raise ValueError(f"Invalid schema version: {version}")
    
    # Reject too many components (reject "v.1.2.3.4")
    if len(parts) > 3:
        raise ValueError(f"Unsupported schema format: {version}")
    
    for p in parts:
        if not p.isdigit():
            raise ValueError(f"Invalid schema version: {version}")

        if len(p) > 1 and p.startswith("0"):
            raise ValueError(f"Invalid schema version (leading zero): {version}")
    
    parts_int = [int(p) for p in parts]
    return SchemaVersion(*parts_int)

def ensure_compatibility(source_version: str, target_version: str) -> None:
    source = parse_version(source_version)
    target = parse_version(target_version)

    if source.major != target.major:
        raise ValueError(
            f"Incompatible schema versions: {source_version} -> {target_version}"
        )

    if target < source:
        raise ValueError(
            f"Backward upgrade not allowed: {source_version} -> {target_version}"
        )