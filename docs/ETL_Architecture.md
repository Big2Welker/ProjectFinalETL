# Integrated Batch and Streaming ETL Architecture — Final Delivery (ODS 6 Water Quality in Colombia)

This architecture implements **Figure 1 (Integrated Batch and Streaming Architecture)** from the Final Delivery specification, combining an automated **Apache Airflow 3.1.8 Batch ETL Pipeline** (with 3 **Great Expectations** validation gates and a **MySQL 8.0 Dimensional Data Warehouse**) with a low-latency **Apache Kafka Event Streaming Pipeline** (`Producer -> Topic -> Consumer -> Streaming Metrics Store -> Real-Time Dashboard`).

```mermaid
flowchart TD
    subgraph SOURCES["1. Data Sources (Static & Dynamic)"]
        CSV["Primary Static Dataset (INS SIVICAP CSV)\n19,160 raw rows (2007-2024)"]
        API["Dynamic Public API (API Colombia REST v1)\n/Region (6), /Department (33),\n/City (1,123), /IndigenousReservation (829)"]
    end

    subgraph AIRFLOW["2. Apache Airflow 3.1.8 — Batch ETL & Great Expectations (3 Gates)"]
        EXT_P["Task: extract_primary\n(Raw CSV + MD5 Manifest)"]
        EXT_A["Task: extract_api\n(Paginated HTTP + Retry Backoff)"]
        GX1["Task: validate_primary_raw\n(GX Gate 1: DQ_01 to DQ_04)"]
        GX2["Task: validate_api_raw\n(GX Gate 2: DQ_05 to DQ_09)"]
        TRANS["Task: transform_and_clean_and_integrate\n(Filter 569 #TODOS, Preserve True NULLs,\nNormalize & Join INS + API: 98.12% Match)"]
        GX3["Task: validate_prepared\n(GX Gate 3: DQ_10 to DQ_16)"]
        LOAD["Task: load_dimensional_dw\n(Transactional Idempotent Load)"]
    end

    subgraph DW["3. MySQL 8.0 Data Warehouse (Star Schema)"]
        FACT[("fact_irca (18,591 rows)\ndim_geografia (1,095 rows)\ndim_tiempo (18 rows)")]
    end

    subgraph BATCH_BI["4A. Historical Analytical Dashboard"]
        PBI["Power BI Dashboard (dashboard.pbix)\nHistorical & Integrated KPIs (R1 to R7)"]
    end

    subgraph KAFKA_STREAM["4B. Apache Kafka Streaming Pipeline (Low-Latency)"]
        PROD["Python Kafka Producer\n(Stateful Delta Filter: |ΔIRCA| >= 0.5 pts\nSuppresses Unchanged KPI Repeats)"]
        TOPIC[["Kafka Topic: irca.water.telemetry.v1\n3 Partitions · Key: codigo_municipio"]]
        CONS["Python Kafka Consumer\n(Event Contract Validator + Offset Tracking)"]
        DLQ[("stream_dlq_invalid_events\n(Dead-Letter Queue for Rejected Events)")]
    end

    subgraph STREAM_STORE["5. MySQL Streaming Metrics Store"]
        STORE[("stream_events_log\nstream_metrics_realtime (S1-S3)\nstream_regional_metrics (S4)")]
    end

    subgraph RT_BI["6. Real-Time Dashboard"]
        RTDASH["Live Auto-Refreshing Web Dashboard (2.0s Poll)\n& Static Snapshot (realtime_dashboard_snapshot.html)"]
    end

    CSV --> EXT_P --> GX1
    API --> EXT_A --> GX2
    GX1 --> TRANS
    GX2 --> TRANS
    TRANS --> GX3 --> LOAD --> FACT
    FACT --> PBI
    FACT --> PROD --> TOPIC --> CONS
    CONS -- "Contract Violation\n(e.g. IRCA=165%)" --> DLQ
    CONS -- "Valid Event" --> STORE --> RTDASH
```
