"""
Python Kafka Consumer for Water Quality (IRCA) Streaming Analytics (Section 2.6 & 2.7).
Uses `confluent_kafka.Consumer` (librdkafka) for reliable offset tracking and interruption recovery:
- Subscribes to Kafka topic `irca.water.telemetry.v1`.
- Validates incoming events against the formal Event Contract (`streaming/event_contract.py`).
- Routes invalid events to `stream_dlq_invalid_events` (Dead-Letter Queue) without crashing.
- Continuously updates 4+ streaming metrics in the MySQL Streaming Metrics Store:
  * Metric 1: Rolling Window Average IRCA (General & Rural, last 50 events) + Valid/Rejected counters.
  * Metric 2: Real-Time High-Risk (>35%) & Sanitarily Inviable (>80%) Municipal Alert Counters.
  * Metric 3: Indigenous Territory Real-Time IRCA & Vulnerability Gap (Con vs. Sin Resguardo Indígena).
  * Metric 4: Real-Time Regional Risk Aggregation across Colombia's 6 Natural Regions.
"""

import os
import sys
import json
import time
import argparse
from datetime import datetime, timezone
from typing import Dict, Any, Optional

import pymysql
from dotenv import load_dotenv
from confluent_kafka import Consumer, KafkaError

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from streaming.event_contract import (
    KAFKA_TOPIC_TELEMETRY,
    KAFKA_CONSUMER_GROUP,
    validate_streaming_event,
)

load_dotenv(os.path.join(PROJECT_ROOT, ".env"))


def get_db_connection() -> pymysql.connections.Connection:
    return pymysql.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "3306")),
        user=os.getenv("DB_USER", "root"),
        password=os.getenv("DB_PASSWORD", "root"),
        database=os.getenv("DB_NAME", "dw_calidad_agua_colombia"),
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
    )


def ensure_streaming_tables_exist(conn: pymysql.connections.Connection) -> None:
    """Creates the Streaming Metrics Store tables in MySQL if not already present."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS stream_events_log (
                event_id VARCHAR(64) PRIMARY KEY,
                event_timestamp VARCHAR(40) NOT NULL,
                processed_at_utc VARCHAR(40) NOT NULL,
                event_key VARCHAR(20) NOT NULL,
                event_type VARCHAR(40) NOT NULL,
                simulation_mode VARCHAR(40) NOT NULL,
                codigo_municipio VARCHAR(10) NOT NULL,
                municipio VARCHAR(150) NOT NULL,
                departamento VARCHAR(150) NOT NULL,
                region_geografica VARCHAR(100) NOT NULL,
                anio_referencia INT NOT NULL,
                irca_general DECIMAL(6,2) NOT NULL,
                irca_urbano DECIMAL(6,2) NULL,
                irca_rural DECIMAL(6,2) NULL,
                delta_irca_prev DECIMAL(6,2) NOT NULL,
                nivel_riesgo VARCHAR(50) NOT NULL,
                tiene_resguardo_indigena TINYINT(1) NOT NULL,
                total_resguardos_indigenas INT NOT NULL,
                poblacion_depto_api BIGINT NOT NULL,
                kafka_partition INT NOT NULL DEFAULT 0,
                kafka_offset BIGINT NOT NULL DEFAULT 0
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS stream_metrics_realtime (
                metric_snapshot_id INT AUTO_INCREMENT PRIMARY KEY,
                updated_at_utc VARCHAR(40) NOT NULL,
                total_valid_events INT NOT NULL,
                total_rejected_events INT NOT NULL,
                rolling_avg_irca_general DECIMAL(6,2) NOT NULL,
                rolling_avg_irca_rural DECIMAL(6,2) NOT NULL,
                critical_alerts_count INT NOT NULL,
                inviable_sanitaria_count INT NOT NULL,
                indigenous_municipalities_alert_count INT NOT NULL,
                indigenous_rolling_avg_irca DECIMAL(6,2) NOT NULL,
                non_indigenous_rolling_avg_irca DECIMAL(6,2) NOT NULL,
                indigenous_gap_pts DECIMAL(6,2) NOT NULL,
                last_event_municipio VARCHAR(150) NOT NULL,
                last_event_departamento VARCHAR(150) NOT NULL,
                last_event_irca DECIMAL(6,2) NOT NULL,
                last_event_riesgo VARCHAR(50) NOT NULL
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS stream_regional_metrics (
                region_geografica VARCHAR(100) PRIMARY KEY,
                events_processed INT NOT NULL,
                avg_irca_general DECIMAL(6,2) NOT NULL,
                avg_irca_rural DECIMAL(6,2) NULL,
                max_irca_recorded DECIMAL(6,2) NOT NULL,
                critical_alerts_count INT NOT NULL,
                indigenous_alerts_count INT NOT NULL,
                updated_at_utc VARCHAR(40) NOT NULL
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS stream_dlq_invalid_events (
                dlq_id INT AUTO_INCREMENT PRIMARY KEY,
                rejected_at_utc VARCHAR(40) NOT NULL,
                event_id VARCHAR(64) NULL,
                event_key VARCHAR(64) NULL,
                violation_code VARCHAR(80) NOT NULL,
                violation_detail TEXT NOT NULL,
                raw_payload TEXT NOT NULL,
                kafka_partition INT NOT NULL DEFAULT 0,
                kafka_offset BIGINT NOT NULL DEFAULT 0
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
        """)
    conn.commit()


class IRCATelemetryConsumer:
    """
    Kafka Consumer that validates incoming events against the contract, persists valid events,
    routes invalid events to the DLQ, and updates real-time streaming KPIs in MySQL.
    """

    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        group_id: str = KAFKA_CONSUMER_GROUP,
        auto_offset_reset: str = "earliest",
        reset_store: bool = False,
    ):
        self.bootstrap_servers = bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
        self.group_id = group_id
        self.conn = get_db_connection()
        ensure_streaming_tables_exist(self.conn)
        if reset_store:
            self.reset_streaming_store()

        self.consumer = Consumer({
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.group_id,
            "auto.offset.reset": auto_offset_reset,
            "enable.auto.commit": False,
        })
        self.consumer.subscribe([KAFKA_TOPIC_TELEMETRY])
        self.session_valid_count = 0
        self.session_rejected_count = 0

    def reset_streaming_store(self) -> None:
        with self.conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE stream_events_log;")
            cur.execute("TRUNCATE TABLE stream_metrics_realtime;")
            cur.execute("TRUNCATE TABLE stream_regional_metrics;")
            cur.execute("TRUNCATE TABLE stream_dlq_invalid_events;")
        self.conn.commit()

    def _record_invalid_event(
        self,
        raw_bytes: bytes,
        parsed_dict: Optional[Dict[str, Any]],
        violation_code: str,
        violation_detail: str,
        partition: int,
        offset: int,
    ) -> None:
        now_utc = datetime.now(timezone.utc).isoformat()
        raw_text = raw_bytes.decode("utf-8", errors="replace")[:2000] if raw_bytes else ""
        event_id = str(parsed_dict.get("event_id"))[:64] if isinstance(parsed_dict, dict) and parsed_dict.get("event_id") else None
        event_key = str(parsed_dict.get("event_key"))[:64] if isinstance(parsed_dict, dict) and parsed_dict.get("event_key") else None

        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO stream_dlq_invalid_events (
                    rejected_at_utc, event_id, event_key, violation_code,
                    violation_detail, raw_payload, kafka_partition, kafka_offset
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (now_utc, event_id, event_key, violation_code, violation_detail, raw_text, partition, offset),
            )
        self.conn.commit()
        self.session_rejected_count += 1

    def _update_streaming_metrics_store(self, latest_event: Dict[str, Any]) -> Dict[str, Any]:
        now_utc = datetime.now(timezone.utc).isoformat()
        with self.conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS cnt FROM stream_events_log;")
            total_valid = int(cur.fetchone()["cnt"])

            cur.execute("SELECT COUNT(*) AS cnt FROM stream_dlq_invalid_events;")
            total_rejected = int(cur.fetchone()["cnt"])

            cur.execute("""
                SELECT
                    COALESCE(ROUND(AVG(irca_general), 2), 0.0) AS rolling_gen,
                    COALESCE(ROUND(AVG(irca_rural), 2), 0.0) AS rolling_rur
                FROM (
                    SELECT irca_general, irca_rural
                    FROM stream_events_log
                    ORDER BY processed_at_utc DESC
                    LIMIT 50
                ) recent_win;
            """)
            win_row = cur.fetchone()
            rolling_gen = float(win_row["rolling_gen"] or 0.0)
            rolling_rur = float(win_row["rolling_rur"] or 0.0)

            cur.execute("""
                SELECT
                    SUM(CASE WHEN irca_general > 35.0 THEN 1 ELSE 0 END) AS crit_cnt,
                    SUM(CASE WHEN irca_general > 80.0 THEN 1 ELSE 0 END) AS inv_cnt,
                    SUM(CASE WHEN tiene_resguardo_indigena = 1 AND irca_general > 35.0 THEN 1 ELSE 0 END) AS ind_crit_cnt,
                    COALESCE(ROUND(AVG(CASE WHEN tiene_resguardo_indigena = 1 THEN irca_general END), 2), 0.0) AS ind_avg,
                    COALESCE(ROUND(AVG(CASE WHEN tiene_resguardo_indigena = 0 THEN irca_general END), 2), 0.0) AS non_ind_avg
                FROM stream_events_log;
            """)
            agg = cur.fetchone()
            crit_cnt = int(agg["crit_cnt"] or 0)
            inv_cnt = int(agg["inv_cnt"] or 0)
            ind_crit_cnt = int(agg["ind_crit_cnt"] or 0)
            ind_avg = float(agg["ind_avg"] or 0.0)
            non_ind_avg = float(agg["non_ind_avg"] or 0.0)
            ind_gap = round(ind_avg - non_ind_avg, 2)

            cur.execute(
                """
                INSERT INTO stream_metrics_realtime (
                    updated_at_utc, total_valid_events, total_rejected_events,
                    rolling_avg_irca_general, rolling_avg_irca_rural,
                    critical_alerts_count, inviable_sanitaria_count,
                    indigenous_municipalities_alert_count, indigenous_rolling_avg_irca,
                    non_indigenous_rolling_avg_irca, indigenous_gap_pts,
                    last_event_municipio, last_event_departamento,
                    last_event_irca, last_event_riesgo
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    now_utc,
                    total_valid,
                    total_rejected,
                    rolling_gen,
                    rolling_rur,
                    crit_cnt,
                    inv_cnt,
                    ind_crit_cnt,
                    ind_avg,
                    non_ind_avg,
                    ind_gap,
                    latest_event["municipio"],
                    latest_event["departamento"],
                    float(latest_event["irca_general"]),
                    latest_event["nivel_riesgo"],
                ),
            )

            cur.execute("""
                REPLACE INTO stream_regional_metrics (
                    region_geografica, events_processed, avg_irca_general,
                    avg_irca_rural, max_irca_recorded, critical_alerts_count,
                    indigenous_alerts_count, updated_at_utc
                )
                SELECT
                    region_geografica,
                    COUNT(*) AS events_processed,
                    ROUND(AVG(irca_general), 2) AS avg_irca_general,
                    ROUND(AVG(irca_rural), 2) AS avg_irca_rural,
                    ROUND(MAX(irca_general), 2) AS max_irca_recorded,
                    SUM(CASE WHEN irca_general > 35.0 THEN 1 ELSE 0 END) AS critical_alerts_count,
                    SUM(CASE WHEN tiene_resguardo_indigena = 1 AND irca_general > 35.0 THEN 1 ELSE 0 END) AS indigenous_alerts_count,
                    %s AS updated_at_utc
                FROM stream_events_log
                GROUP BY region_geografica;
            """, (now_utc,))

        self.conn.commit()
        return {
            "updated_at_utc": now_utc,
            "total_valid_events": total_valid,
            "total_rejected_events": total_rejected,
            "rolling_avg_irca_general": rolling_gen,
            "rolling_avg_irca_rural": rolling_rur,
            "critical_alerts_count": crit_cnt,
            "inviable_sanitaria_count": inv_cnt,
            "indigenous_municipalities_alert_count": ind_crit_cnt,
            "indigenous_rolling_avg_irca": ind_avg,
            "non_indigenous_rolling_avg_irca": non_ind_avg,
            "indigenous_gap_pts": ind_gap,
            "last_event_municipio": latest_event["municipio"],
            "last_event_irca": float(latest_event["irca_general"]),
        }

    def consume_batch(self, max_messages: int = 200, empty_polls_limit: int = 4) -> Dict[str, Any]:
        """Consumes up to `max_messages` from Kafka, validates contract, commits offsets, and updates MySQL."""
        latest_snapshot = None
        processed_in_call = 0
        empty_polls = 0

        while processed_in_call < max_messages and empty_polls < empty_polls_limit:
            msg = self.consumer.poll(timeout=1.5)
            if msg is None:
                empty_polls += 1
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    empty_polls += 1
                    continue
                raise RuntimeError(f"Kafka Consumer error: {msg.error()}")

            empty_polls = 0
            raw_val = msg.value()
            is_valid, parsed, v_code, v_detail = validate_streaming_event(raw_val)
            if not is_valid:
                self._record_invalid_event(
                    raw_bytes=raw_val,
                    parsed_dict=parsed,
                    violation_code=v_code or "CONTRACT_VIOLATION",
                    violation_detail=v_detail or "Unknown contract error",
                    partition=msg.partition(),
                    offset=msg.offset(),
                )
                # Refresh total_rejected_events in latest snapshot if one exists
                with self.conn.cursor() as cur:
                    cur.execute("SELECT COUNT(*) AS cnt FROM stream_dlq_invalid_events;")
                    tot_rej = int(cur.fetchone()["cnt"])
                    cur.execute(
                        "UPDATE stream_metrics_realtime SET total_rejected_events = %s ORDER BY metric_snapshot_id DESC LIMIT 1;",
                        (tot_rej,),
                    )
                self.conn.commit()
            else:
                now_utc = datetime.now(timezone.utc).isoformat()
                with self.conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT IGNORE INTO stream_events_log (
                            event_id, event_timestamp, processed_at_utc, event_key,
                            event_type, simulation_mode, codigo_municipio, municipio,
                            departamento, region_geografica, anio_referencia,
                            irca_general, irca_urbano, irca_rural, delta_irca_prev,
                            nivel_riesgo, tiene_resguardo_indigena, total_resguardos_indigenas,
                            poblacion_depto_api, kafka_partition, kafka_offset
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            parsed["event_id"],
                            parsed["event_timestamp"],
                            now_utc,
                            parsed["event_key"],
                            parsed["event_type"],
                            parsed["simulation_mode"],
                            parsed["codigo_municipio"],
                            parsed["municipio"],
                            parsed["departamento"],
                            parsed["region_geografica"],
                            int(parsed["anio_referencia"]),
                            float(parsed["irca_general"]),
                            float(parsed["irca_urbano"]) if parsed.get("irca_urbano") is not None else None,
                            float(parsed["irca_rural"]) if parsed.get("irca_rural") is not None else None,
                            float(parsed["delta_irca_prev"]),
                            parsed["nivel_riesgo"],
                            int(parsed["tiene_resguardo_indigena"]),
                            int(parsed["total_resguardos_indigenas"]),
                            int(parsed["poblacion_depto_api"]),
                            msg.partition(),
                            msg.offset(),
                        ),
                    )
                self.conn.commit()
                self.session_valid_count += 1
                latest_snapshot = self._update_streaming_metrics_store(parsed)

            self.consumer.commit(message=msg, asynchronous=False)
            processed_in_call += 1

        return {
            "session_valid_count": self.session_valid_count,
            "session_rejected_count": self.session_rejected_count,
            "latest_snapshot": latest_snapshot,
        }

    def close(self) -> None:
        self.consumer.close()
        self.conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Kafka Consumer for IRCA Streaming Analytics")
    parser.add_argument("--max-messages", type=int, default=200, help="Maximum messages to consume before exiting")
    parser.add_argument("--reset-store", action="store_true", help="Truncate streaming tables before starting")
    args = parser.parse_args()

    consumer = IRCATelemetryConsumer(reset_store=args.reset_store)
    result = consumer.consume_batch(max_messages=args.max_messages)
    consumer.close()
    print(f"[KafkaConsumer] Summary: {json.dumps(result, ensure_ascii=False, indent=2)}")


if __name__ == "__main__":
    main()
