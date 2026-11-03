"""Source contracts and strict, source-aware row normalization.

CSV represents every value as text; integer fields accept only exact integer
values or integer text. Both formats normalize to the same canonical payload.
"""

from __future__ import annotations

import json
import math
import os
import re
from datetime import date, datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any


class ContractError(ValueError):
    """A source payload violates a declared schema or local business invariant."""


INT32_MAX = 2**31 - 1
INT64_MAX = 2**63 - 1
BUSINESS_KEY_MAX_BYTES = 2000


def validate_text(value: str, field: str = "text") -> None:
    """Reject strings that PostgreSQL UTF-8 text/JSONB cannot represent."""
    if "\x00" in value:
        raise ContractError(f"{field}: NUL characters cannot be stored in PostgreSQL text/JSONB")
    if any(0xD800 <= ord(character) <= 0xDFFF for character in value):
        raise ContractError(f"{field}: unpaired Unicode surrogates cannot be stored in PostgreSQL text/JSONB")


def validate_jsonb(value: Any) -> None:
    """Check the complete payload, including additive fields, before DB writes."""
    if isinstance(value, str):
        validate_text(value, "JSON string")
    elif isinstance(value, dict):
        for key, child in value.items():
            if not isinstance(key, str):
                raise ContractError("JSON object keys must be strings")
            validate_text(key, "JSON object key")
            validate_jsonb(child)
    elif isinstance(value, list):
        for child in value:
            validate_jsonb(child)
    elif isinstance(value, float) and not math.isfinite(value):
        raise ContractError("Non-finite numbers cannot be stored in PostgreSQL JSONB")
    elif value is not None and type(value) not in {int, float, bool}:
        raise ContractError(f"Unsupported JSON value type: {type(value).__name__}")


def _business_key(values: list[Any]) -> str:
    key = str(values[0]) if len(values) == 1 else json.dumps(values, separators=(",", ":"))
    # B-tree keys have a physical size limit. Leave room for source/version and
    # index overhead instead of allowing a nominal string to fail in PostgreSQL.
    if len(key.encode("utf-8")) > BUSINESS_KEY_MAX_BYTES:
        raise ContractError(f"business key: exceeds {BUSINESS_KEY_MAX_BYTES} UTF-8 bytes")
    return key


CONTRACT_ROOT = Path(__file__).resolve().parent / "source_contracts"
if not CONTRACT_ROOT.is_dir():
    repository_root = Path(os.getenv("COMMERCE_ROOT", Path.cwd() if (Path.cwd() / "contracts").is_dir() else Path(__file__).resolve().parents[2]))
    CONTRACT_ROOT = repository_root / "contracts"


@lru_cache(maxsize=8)
def load_contract(source: str) -> dict[str, Any]:
    """Return one known source contract; never interpolate unknown source paths."""
    if source not in {"order_lines", "refunds", "inventory", "products", "locations"}:
        raise ContractError(f"unknown source: {source!r}")
    return json.loads((CONTRACT_ROOT / f"{source}.json").read_text(encoding="utf-8"))


def _normalize(field: str, value: Any, spec: dict[str, Any]) -> Any:
    if value is None or value == "":
        if spec.get("nullable", False):
            return None
        raise ContractError(f"{field}: null or empty value is forbidden")
    kind = spec["type"]
    if kind == "integer":
        # bool subclasses int in Python, so use exact types here.
        if type(value) is int:
            normalized = value
        elif isinstance(value, str) and re.fullmatch(r"-?(0|[1-9][0-9]*)", value):
            try:
                normalized = int(value)
            except ValueError as exc:
                raise ContractError(f"{field}: integer exceeds the client conversion limit") from exc
        else:
            raise ContractError(f"{field}: expected integer, got {value!r}")
        if normalized < spec.get("minimum", normalized):
            raise ContractError(f"{field}: must be >= {spec['minimum']}")
        maximum = spec.get("maximum", INT32_MAX if field == "version" else INT64_MAX)
        if normalized > maximum:
            raise ContractError(f"{field}: must be <= {maximum}")
        return normalized
    if not isinstance(value, str):
        raise ContractError(f"{field}: expected {kind} string, got {type(value).__name__}")
    validate_text(value, field)
    if kind == "date":
        try:
            normalized_date = date.fromisoformat(value)
        except ValueError as exc:
            raise ContractError(f"{field}: expected YYYY-MM-DD date") from exc
        if normalized_date.isoformat() != value:
            raise ContractError(f"{field}: expected YYYY-MM-DD date")
        return value
    if kind == "timestamp":
        try:
            timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ContractError(f"{field}: invalid ISO timestamp") from exc
        if timestamp.tzinfo is None or timestamp.utcoffset() != timezone.utc.utcoffset(timestamp):
            raise ContractError(f"{field}: an explicit UTC offset is required")
        return timestamp.astimezone(timezone.utc).isoformat()
    if kind != "string":
        raise ContractError(f"{field}: unsupported contract type {kind}")
    if value.strip() != value or not value:
        raise ContractError(f"{field}: expected nonempty string without surrounding whitespace")
    if "accepted_values" in spec and value not in spec["accepted_values"]:
        raise ContractError(f"{field}: must be one of {spec['accepted_values']}")
    return value


def validate_record(
    source: str, record: dict[str, Any], *, schema_version: int = 1
) -> tuple[dict[str, Any], str]:
    """Return (canonical payload, stable business key), or raise ContractError.

    Documented additive fields survive normalization and raw provenance retention.
    Cross-record constraints (references, cumulative refunds, lifecycle consistency
    between order lines) are warehouse quality gates rather than row validation.
    """
    contract = load_contract(source)
    if type(schema_version) is not int or schema_version != contract["schema_version"]:
        raise ContractError(f"{source}: unsupported schema version {schema_version!r}")
    if not isinstance(record, dict):
        raise ContractError(f"{source}: expected object row")
    validate_jsonb(record)
    payload = dict(record)
    for name, spec in contract["fields"].items():
        if name not in record:
            raise ContractError(f"{name}: required field missing")
        payload[name] = _normalize(name, record[name], spec)
    if source == "order_lines":
        if payload["quantity"] * payload["unit_price_cents"] > INT64_MAX:
            raise ContractError("quantity * unit_price_cents: exceeds PostgreSQL bigint storage range")
        if payload["discount_cents"] > payload["quantity"] * payload["unit_price_cents"]:
            raise ContractError("discount_cents: exceeds gross merchandise subtotal")
        status = payload["status"]
        paid, fulfilled = payload["paid_at"], payload["fulfilled_at"]
        if status in {"paid", "fulfilled"} and paid is None:
            raise ContractError("paid_at: required for paid or fulfilled order")
        if status == "fulfilled" and fulfilled is None:
            raise ContractError("fulfilled_at: required for fulfilled order")
        if status != "fulfilled" and fulfilled is not None:
            raise ContractError("fulfilled_at: only fulfilled orders may have fulfillment timestamp")
        if fulfilled is not None and (paid is None or datetime.fromisoformat(fulfilled) < datetime.fromisoformat(paid)):
            raise ContractError("fulfilled_at: precedes payment timestamp")
        if paid is not None and datetime.fromisoformat(paid).date().isoformat() != payload["sale_date"]:
            raise ContractError("paid_at: UTC payment date must equal sale_date")
    key_values = [payload[key] for key in contract["business_key"]]
    business_key = _business_key(key_values)
    return payload, business_key


def quarantine_identity(source: str, record: Any) -> tuple[str | None, int | None]:
    """Recover only a rejected row's normalized key/version when possible.

    Invalid non-key fields and unsupported source schema identifiers do not hide
    a readable known business key. Missing/invalid keys remain unkeyed and need
    an explicit audited operator decision; file position is never an identity.
    """
    if not isinstance(record, dict):
        return None, None
    contract = load_contract(source)
    business_key = None
    source_version = None
    try:
        values = [_normalize(field, record[field], contract["fields"][field]) for field in contract["business_key"]]
        business_key = _business_key(values)
    except (KeyError, ContractError):
        pass
    try:
        source_version = _normalize("version", record["version"], contract["fields"]["version"])
    except (KeyError, ContractError):
        pass
    return business_key, source_version
