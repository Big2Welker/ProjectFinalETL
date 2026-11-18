"""
Python Kafka Producer for Water Quality (IRCA) Streaming Telemetry (Section 2.6).
Uses `confluent_kafka.Producer` (librdkafka) for high-reliability event publishing:
- Queries validated records from the MySQL Data Warehouse (`fact_irca` + `dim_geografia` + `dim_tiempo`).
- Avoids repeatedly publishing unchanged KPI values: tracks previous municipal IRCA state and
  only publishes when a new baseline arrives, when |ΔIRCA| >= 0.5 pts, or when a risk-level transition occurs.
- Publishes JSON events keyed by `codigo_municipio` to Kafka topic `irca.water.telemetry.v1`.
- Supports injecting invalid events to demonstrate consumer contract validation and DLQ routing.
"""

import os
import sys
import json
import time
import uuid
import argparse
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

import pymysql
from dotenv import load_dotenv
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from streaming.event_contract import KAFKA_TOPIC_TELEMETRY, classify_irca_risk

load_dotenv(os.path.join(PROJECT_ROOT, ".env"))


def get_kafka_bootstrap_servers() -> str:
    return os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")


def ensure_topic_exists(bootstrap_servers: str, topic_name: str = KAFKA_TOPIC_TELEMETRY, num_partitions: int = 3) -> None:
    """Ensures the Kafka topic exists before publishing."""
    try:
        admin = AdminClient({"bootstrap.servers": bootstrap_servers})
        md = admin.list_topics(timeout=5)
        if topic_name not in md.topics:
            fs = admin.create_topics([NewTopic(topic_name, num_partitions=num_partitions, replication_factor=1)])
            for _, f in fs.items():
                try:
                    f.result(timeout=5)
                except Exception:
                    pass
    except Exception:
        pass


def fetch_dw_records_for_replay(limit: int = 250, offset: int = 0) -> List[Dict[str, Any]]:
    """
    Fetches validated historical records from the MySQL Data Warehouse (`fact_irca` joined with
    `dim_geografia` and `dim_tiempo`) ordered chronologically to simulate low-latency event arrival.
    """
    host = os.getenv("DB_HOST", "localhost")
    port = int(os.getenv("DB_PORT", "3306"))
    user = os.getenv("DB_USER", "root")
    password = os.getenv("DB_PASSWORD", "root")
    db_name = os.getenv("DB_NAME", "dw_calidad_agua_colombia")

    conn = pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=db_name,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )
    try:
        with conn.cursor() as cur:
            sql = """
                SELECT
                    g.codigo_municipio,
                    g.municipio,
                    g.departamento,
                    g.region_geografica,
                    g.poblacion_depto_api,
                    g.tiene_resguardo_indigena,
                    g.total_resguardos_indigenas,
                    t.anio AS anio_referencia,
                    f.irca_general,
                    f.irca_urbano,
                    f.irca_rural,
                    f.nivel_riesgo_general
                FROM fact_irca f
                INNER JOIN dim_geografia g ON f.fk_geografia = g.sk_geografia
                INNER JOIN dim_tiempo t ON f.fk_tiempo = t.sk_tiempo
                WHERE t.anio >= 2021
                ORDER BY t.anio ASC, f.irca_general DESC, g.codigo_municipio ASC
                LIMIT %s OFFSET %s
            """
            cur.execute(sql, (limit, offset))
            return cur.fetchall()
    finally:
        conn.close()


class IRCATelemetryProducer:
    """
    Stateful Kafka Producer that suppresses unchanged KPI values and publishes
    meaningful water quality updates to `irca.water.telemetry.v1`.
    """

    def __init__(self, bootstrap_servers: Optional[str] = None, min_delta_threshold: float = 0.5):
        self.bootstrap_servers = bootstrap_servers or get_kafka_bootstrap_servers()
        self.min_delta_threshold = min_delta_threshold
        ensure_topic_exists(self.bootstrap_servers, KAFKA_TOPIC_TELEMETRY)
        self.producer = Producer({
            "bootstrap.servers": self.bootstrap_servers,
            "acks": "all",
            "retries": 3,
        })
        self.municipal_state: Dict[str, Dict[str, Any]] = {}
        self.published_count = 0
        self.suppressed_unchanged_count = 0
        self._last_delivery = {"partition": 0, "offset": 0}

    def _delivery_cb(self, err, msg):
        if err is not None:
            raise RuntimeError(f"Kafka delivery failed: {err}")
        self._last_delivery = {"partition": msg.partition(), "offset": msg.offset()}

    def build_and_publish_record(self, row: Dict[str, Any], delay_sec: float = 0.005) -> Optional[Dict[str, Any]]:
        codigo_mpio = str(row["codigo_municipio"]).strip()
        irca_gen = round(float(row["irca_general"]), 2)
        irca_urb = round(float(row["irca_urbano"]), 2) if row.get("irca_urbano") is not None else None
        irca_rur = round(float(row["irca_rural"]), 2) if row.get("irca_rural") is not None else None
        nivel_riesgo = str(row.get("nivel_riesgo_general") or classify_irca_risk(irca_gen)).strip().upper()

        prev_state = self.municipal_state.get(codigo_mpio)
        if prev_state is not None:
            delta_irca = round(irca_gen - prev_state["irca_general"], 2)
            same_category = nivel_riesgo == prev_state["nivel_riesgo"]
            if abs(delta_irca) < self.min_delta_threshold and same_category:
                self.suppressed_unchanged_count += 1
                return None
            event_type = "CRITICAL_RISK_SPIKE" if irca_gen > 35.0 else "DELTA_METRIC_UPDATE"
        else:
            delta_irca = 0.0
            event_type = "CRITICAL_RISK_SPIKE" if irca_gen > 35.0 else "BASELINE_SAMPLE"

        self.municipal_state[codigo_mpio] = {
            "irca_general": irca_gen,
            "nivel_riesgo": nivel_riesgo,
        }

        event_payload = {
            "event_id": str(uuid.uuid4()),
            "event_timestamp": datetime.now(timezone.utc).isoformat(),
            "event_key": codigo_mpio,
            "event_type": event_type,
            "simulation_mode": "HISTORICAL_REPLAY_SIMULATION",
            "codigo_municipio": codigo_mpio,
            "municipio": str(row["municipio"]),
            "departamento": str(row["departamento"]),
            "region_geografica": str(row["region_geografica"]),
            "anio_referencia": int(row["anio_referencia"]),
            "irca_general": irca_gen,
            "irca_urbano": irca_urb,
            "irca_rural": irca_rur,
            "delta_irca_prev": delta_irca,
            "nivel_riesgo": nivel_riesgo,
            "tiene_resguardo_indigena": int(row["tiene_resguardo_indigena"]),
            "total_resguardos_indigenas": int(row["total_resguardos_indigenas"]),
            "poblacion_depto_api": int(row["poblacion_depto_api"]),
        }

        raw_bytes = json.dumps(event_payload, ensure_ascii=False).encode("utf-8")
        self.producer.produce(
            KAFKA_TOPIC_TELEMETRY,
            key=codigo_mpio.encode("utf-8"),
            value=raw_bytes,
            callback=self._delivery_cb,
        )
        self.producer.poll(0)
        self.published_count += 1
        if delay_sec > 0:
            time.sleep(delay_sec)
        return {
            "event_id": event_payload["event_id"],
            "codigo_municipio": codigo_mpio,
            "municipio": event_payload["municipio"],
            "irca_general": irca_gen,
            "event_type": event_type,
        }

    def publish_invalid_test_events(self) -> List[Dict[str, Any]]:
        """
        Publishes 3 distinct contract-violating events to test Consumer resilience & DLQ routing:
        1. Physical domain violation (`irca_general = 165.0` > 100.0).
        2. Missing required field (`codigo_municipio = None`).
        3. Malformed raw JSON bytes.
        """
        invalid_events_summary = []

        bad_event_1 = {
            "event_id": str(uuid.uuid4()),
            "event_timestamp": datetime.now(timezone.utc).isoformat(),
            "event_key": "27073",
            "event_type": "CRITICAL_RISK_SPIKE",
            "simulation_mode": "HISTORICAL_REPLAY_SIMULATION",
            "codigo_municipio": "27073",
            "municipio": "BAGADÓ",
            "departamento": "CHOCÓ",
            "region_geografica": "PACÍFICO",
            "anio_referencia": 2024,
            "irca_general": 165.00,
            "irca_urbano": 85.0,
            "irca_rural": 100.0,
            "delta_irca_prev": 65.0,
            "nivel_riesgo": "INVIABLE SANITARIAMENTE",
            "tiene_resguardo_indigena": 1,
            "total_resguardos_indigenas": 4,
            "poblacion_depto_api": 553519,
        }
        self.producer.produce(
            KAFKA_TOPIC_TELEMETRY,
            key=b"27073",
            value=json.dumps(bad_event_1).encode("utf-8"),
            callback=self._delivery_cb,
        )
        self.producer.flush(5)
        invalid_events_summary.append({
            "case": "DOMAIN_VIOLATION_IRCA_RANGE (irca_general=165.0)",
            "event_id": bad_event_1["event_id"],
            "partition": self._last_delivery["partition"],
            "offset": self._last_delivery["offset"],
        })

        bad_event_2 = {
            "event_id": str(uuid.uuid4()),
            "event_timestamp": datetime.now(timezone.utc).isoformat(),
            "event_key": "05895",
            "event_type": "DELTA_METRIC_UPDATE",
            "simulation_mode": "HISTORICAL_REPLAY_SIMULATION",
            "codigo_municipio": None,
            "municipio": "ZARAGOZA",
            "departamento": "ANTIOQUIA",
            "region_geografica": "ANDINA",
            "anio_referencia": 2024,
            "irca_general": 96.50,
            "delta_irca_prev": 12.0,
            "nivel_riesgo": "INVIABLE SANITARIAMENTE",
            "tiene_resguardo_indigena": 1,
            "total_resguardos_indigenas": 4,
            "poblacion_depto_api": 6887306,
        }
        self.producer.produce(
            KAFKA_TOPIC_TELEMETRY,
            key=b"05895",
            value=json.dumps(bad_event_2).encode("utf-8"),
            callback=self._delivery_cb,
        )
        self.producer.flush(5)
        invalid_events_summary.append({
            "case": "MISSING_REQUIRED_FIELD (codigo_municipio=None)",
            "event_id": bad_event_2["event_id"],
            "partition": self._last_delivery["partition"],
            "offset": self._last_delivery["offset"],
        })

        corrupt_bytes = b"{corrupt_json_payload: irca_general=NaN, broken_syntax"
        self.producer.produce(
            KAFKA_TOPIC_TELEMETRY,
            key=b"99999",
            value=corrupt_bytes,
            callback=self._delivery_cb,
        )
        self.producer.flush(5)
        invalid_events_summary.append({
            "case": "MALFORMED_JSON_PAYLOAD (corrupt syntax)",
            "event_id": "N/A (raw bytes)",
            "partition": self._last_delivery["partition"],
            "offset": self._last_delivery["offset"],
        })

        return invalid_events_summary

    def close(self) -> None:
        self.producer.flush(10)


def main() -> None:
    parser = argparse.ArgumentParser(description="Kafka Producer for IRCA Water Quality Telemetry")
    parser.add_argument("--limit", type=int, default=120, help="Number of DW records to evaluate for replay")
    parser.add_argument("--offset", type=int, default=0, help="SQL offset in fact_irca")
    parser.add_argument("--delay", type=float, default=0.01, help="Delay in seconds between published events")
    parser.add_argument("--inject-invalid", action="store_true", help="Inject 3 contract-violating events")
    args = parser.parse_args()

    producer = IRCATelemetryProducer()
    rows = fetch_dw_records_for_replay(limit=args.limit, offset=args.offset)
    print(f"[KafkaProducer] Fetched {len(rows)} records from MySQL DW (limit={args.limit}, offset={args.offset}).")

    for r in rows:
        producer.build_and_publish_record(r, delay_sec=args.delay)

    for r in rows[:15]:
        producer.build_and_publish_record(r, delay_sec=0.0)

    invalid_info = []
    if args.inject_invalid:
        invalid_info = producer.publish_invalid_test_events()
        print(f"[KafkaProducer] Injected {len(invalid_info)} invalid events for contract testing.")

    producer.close()
    print(
        f"[KafkaProducer] Completed: published={producer.published_count}, "
        f"suppressed_unchanged={producer.suppressed_unchanged_count}, "
        f"invalid_injected={len(invalid_info)}"
    )


if __name__ == "__main__":
    main()
