"""
Streaming Event Contract Definition and Validator (Section 2.6).
Defines the schema, required fields, data types, domain constraints,
event identifier, timestamp, and Kafka partition key (`codigo_municipio`)
for the `irca.water.telemetry.v1` topic.
"""

import json
from datetime import datetime
from typing import Dict, Any, Tuple, Optional

KAFKA_TOPIC_TELEMETRY = "irca.water.telemetry.v1"
KAFKA_CONSUMER_GROUP = "cg_irca_streaming_analytics_v1"

VALID_RISK_LEVELS = {
    "SIN RIESGO",
    "RIESGO BAJO",
    "RIESGO MEDIO",
    "RIESGO ALTO",
    "INVIABLE SANITARIAMENTE",
}

EVENT_CONTRACT_SPEC = {
    "contract_version": "1.0.0",
    "topic": KAFKA_TOPIC_TELEMETRY,
    "event_key_field": "codigo_municipio",
    "event_identifier_field": "event_id",
    "timestamp_field": "event_timestamp",
    "required_fields": {
        "event_id": "str (UUIDv4 unique identifier)",
        "event_timestamp": "str (ISO-8601 UTC timestamp)",
        "event_key": "str (DANE DIVIPOLA municipality code - partition key)",
        "event_type": "str (BASELINE_SAMPLE | DELTA_METRIC_UPDATE | CRITICAL_RISK_SPIKE)",
        "simulation_mode": "str (HISTORICAL_REPLAY_SIMULATION | LIVE_TELEMETRY)",
        "codigo_municipio": "str (non-empty DIVIPOLA code)",
        "municipio": "str (municipality name)",
        "departamento": "str (department name)",
        "region_geografica": "str (natural region via API Colombia)",
        "anio_referencia": "int (2007..2030)",
        "irca_general": "float (0.0 <= value <= 100.0)",
        "delta_irca_prev": "float (change vs previous reading)",
        "nivel_riesgo": "str (Resolution 2115/2007 official category)",
        "tiene_resguardo_indigena": "int (0 or 1 via API Colombia)",
        "total_resguardos_indigenas": "int (>= 0 via API Colombia)",
        "poblacion_depto_api": "int (> 0 via API Colombia)",
    },
    "optional_nullable_fields": {
        "irca_urbano": "float | None (0.0 <= value <= 100.0 if present)",
        "irca_rural": "float | None (0.0 <= value <= 100.0 if present)",
    },
}


def classify_irca_risk(irca_value: float) -> str:
    """Official Resolution 2115 of 2007 IRCA classification."""
    if irca_value <= 5.0:
        return "SIN RIESGO"
    if irca_value <= 14.0:
        return "RIESGO BAJO"
    if irca_value <= 35.0:
        return "RIESGO MEDIO"
    if irca_value <= 80.0:
        return "RIESGO ALTO"
    return "INVIABLE SANITARIAMENTE"


def validate_streaming_event(raw_bytes_or_dict: Any) -> Tuple[bool, Optional[Dict[str, Any]], Optional[str], Optional[str]]:
    """
    Validates an incoming Kafka message against the formal Event Contract.
    Returns:
        (is_valid, parsed_dict, violation_code, violation_detail)
    """
    if isinstance(raw_bytes_or_dict, (bytes, bytearray)):
        try:
            text_payload = raw_bytes_or_dict.decode("utf-8")
            payload = json.loads(text_payload)
        except Exception as exc:
            return False, None, "MALFORMED_JSON_PAYLOAD", f"Failed to decode JSON payload: {exc}"
    elif isinstance(raw_bytes_or_dict, str):
        try:
            payload = json.loads(raw_bytes_or_dict)
        except Exception as exc:
            return False, None, "MALFORMED_JSON_PAYLOAD", f"Failed to parse JSON string: {exc}"
    elif isinstance(raw_bytes_or_dict, dict):
        payload = raw_bytes_or_dict
    else:
        return False, None, "UNSUPPORTED_PAYLOAD_TYPE", f"Expected JSON bytes/str/dict, got {type(raw_bytes_or_dict)}"

    if not isinstance(payload, dict):
        return False, None, "INVALID_JSON_ROOT", "Event payload root must be a JSON object."

    # 1. Check required fields presence and non-null
    for req_field in EVENT_CONTRACT_SPEC["required_fields"]:
        if req_field not in payload or payload[req_field] is None:
            return False, payload, "MISSING_REQUIRED_FIELD", f"Required field '{req_field}' is missing or null."

    # 2. Validate event_id and event_key
    event_id = str(payload.get("event_id", "")).strip()
    if len(event_id) < 8:
        return False, payload, "INVALID_EVENT_ID", f"event_id '{event_id}' is too short or invalid."

    codigo_mpio = str(payload.get("codigo_municipio", "")).strip()
    event_key = str(payload.get("event_key", "")).strip()
    if not codigo_mpio or codigo_mpio == "#TODOS":
        return False, payload, "INVALID_MUNICIPALITY_CODE", f"Invalid codigo_municipio '{codigo_mpio}'."
    if event_key != codigo_mpio:
        return False, payload, "KEY_MISMATCH", f"event_key '{event_key}' does not match codigo_municipio '{codigo_mpio}'."

    # 3. Validate ISO-8601 timestamp
    ts_str = str(payload.get("event_timestamp", "")).strip()
    try:
        datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    except Exception:
        return False, payload, "INVALID_TIMESTAMP_FORMAT", f"event_timestamp '{ts_str}' is not valid ISO-8601."

    # 4. Validate numeric types & physical IRCA bounds [0.0, 100.0]
    try:
        irca_general = float(payload["irca_general"])
    except (TypeError, ValueError):
        return False, payload, "INVALID_DATA_TYPE_IRCA", f"irca_general '{payload.get('irca_general')}' is not numeric."

    if irca_general < 0.0 or irca_general > 100.0:
        return (
            False,
            payload,
            "DOMAIN_VIOLATION_IRCA_RANGE",
            f"irca_general={irca_general} violates physical range [0.0, 100.0].",
        )

    for opt_field in ("irca_urbano", "irca_rural"):
        val = payload.get(opt_field)
        if val is not None:
            try:
                f_val = float(val)
            except (TypeError, ValueError):
                return False, payload, f"INVALID_DATA_TYPE_{opt_field.upper()}", f"{opt_field}='{val}' is not numeric."
            if f_val < 0.0 or f_val > 100.0:
                return (
                    False,
                    payload,
                    f"DOMAIN_VIOLATION_{opt_field.upper()}_RANGE",
                    f"{opt_field}={f_val} violates physical range [0.0, 100.0].",
                )

    # 5. Validate reference year and demographic/indigenous attributes
    try:
        anio = int(payload["anio_referencia"])
        if anio < 2007 or anio > 2030:
            return False, payload, "DOMAIN_VIOLATION_YEAR", f"anio_referencia={anio} outside [2007, 2030]."
        tiene_resg = int(payload["tiene_resguardo_indigena"])
        if tiene_resg not in (0, 1):
            return False, payload, "DOMAIN_VIOLATION_INDIGENOUS_FLAG", f"tiene_resguardo_indigena={tiene_resg} not in {{0,1}}."
        total_resg = int(payload["total_resguardos_indigenas"])
        if total_resg < 0:
            return False, payload, "DOMAIN_VIOLATION_RESGUARDOS_NEGATIVE", f"total_resguardos_indigenas={total_resg} < 0."
        pob_api = int(payload["poblacion_depto_api"])
        if pob_api <= 0:
            return False, payload, "DOMAIN_VIOLATION_POPULATION", f"poblacion_depto_api={pob_api} <= 0."
    except (TypeError, ValueError) as exc:
        return False, payload, "INVALID_INTEGER_FIELD", f"Integer conversion failed: {exc}"

    # 6. Validate risk category domain
    nivel = str(payload.get("nivel_riesgo", "")).strip().upper()
    if nivel not in VALID_RISK_LEVELS:
        return False, payload, "INVALID_RISK_CATEGORY", f"nivel_riesgo '{nivel}' not in {sorted(VALID_RISK_LEVELS)}."

    return True, payload, None, None
