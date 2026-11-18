-- =================================================================================
-- DDL: INTEGRATED BATCH DATA WAREHOUSE & STREAMING METRICS STORE (FINAL DELIVERY)
-- Engine: MySQL 8.0+ | Database: dw_calidad_agua_colombia
-- =================================================================================

CREATE DATABASE IF NOT EXISTS dw_calidad_agua_colombia
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

USE dw_calidad_agua_colombia;

-- =================================================================================
-- PART 1: BATCH DIMENSIONAL MODEL (STAR SCHEMA - R1 TO R7)
-- =================================================================================

DROP TABLE IF EXISTS fact_irca;
DROP TABLE IF EXISTS dim_geografia;
DROP TABLE IF EXISTS dim_tiempo;
DROP TABLE IF EXISTS etl_batch_audit;

-- 1. Temporal Dimension (Grain: Calendar Year)
CREATE TABLE dim_tiempo (
    sk_tiempo INT PRIMARY KEY COMMENT 'Temporal Surrogate Key',
    anio INT NOT NULL UNIQUE COMMENT 'Measurement Calendar Year (Business Key)',
    decada INT NOT NULL COMMENT 'Decade for historical aggregation',
    es_lustro_reciente TINYINT(1) NOT NULL COMMENT '1 if within recent 5-year window (2020-2024)'
);

-- 2. Conformed Geographic Dimension Enriched with API Colombia (Grain: Municipality DIVIPOLA)
CREATE TABLE dim_geografia (
    sk_geografia INT PRIMARY KEY COMMENT 'Geographic Surrogate Key',
    codigo_departamento VARCHAR(5) NOT NULL COMMENT 'DANE DIVIPOLA Department Code (2 digits)',
    departamento VARCHAR(150) NOT NULL COMMENT 'Standardized Official Department Name',
    codigo_municipio VARCHAR(10) NOT NULL UNIQUE COMMENT 'DANE DIVIPOLA Municipality Code (5 digits - Business Key)',
    municipio VARCHAR(150) NOT NULL COMMENT 'Standardized Official Municipality Name',
    region_geografica VARCHAR(100) NOT NULL COMMENT 'Natural Region of Colombia via API Colombia (/Region)',
    poblacion_depto_api BIGINT NOT NULL COMMENT 'Departmental Population via API Colombia (/Department)',
    superficie_depto_km2_api DECIMAL(12,2) NOT NULL COMMENT 'Departmental Surface Area in km2 via API Colombia',
    densidad_depto_hab_km2 DECIMAL(12,2) NOT NULL COMMENT 'Derived Demographic Density (inhab/km2)',
    total_resguardos_indigenas INT NOT NULL DEFAULT 0 COMMENT 'Indigenous Reservation count in municipality via API Colombia',
    tiene_resguardo_indigena TINYINT(1) NOT NULL DEFAULT 0 COMMENT 'Boolean flag 1/0 indicating indigenous reservation presence'
);

-- 3. Fact_IRCA Fact Table (Grain: Consolidated annual IRCA measurement per municipality and year)
CREATE TABLE fact_irca (
    fk_geografia INT NOT NULL COMMENT 'FK referencing dim_geografia',
    fk_tiempo INT NOT NULL COMMENT 'FK referencing dim_tiempo',
    irca_general DECIMAL(6,2) NOT NULL COMMENT 'Consolidated municipal IRCA [0.00 to 100.00] (100% complete)',
    irca_urbano DECIMAL(6,2) NULL COMMENT 'Urban IRCA (True NULL when unmeasured; NEVER imputed as 0)',
    irca_rural DECIMAL(6,2) NULL COMMENT 'Rural IRCA (True NULL when unmeasured; NEVER imputed as 0)',
    brecha_rural_urbana DECIMAL(6,2) NULL COMMENT 'Difference (irca_rural - irca_urbano) when both exist',
    nivel_riesgo_general VARCHAR(50) NOT NULL COMMENT 'Normative Resolution 2115/2007 risk category',
    PRIMARY KEY (fk_geografia, fk_tiempo),
    CONSTRAINT fk_fact_geo FOREIGN KEY (fk_geografia) REFERENCES dim_geografia(sk_geografia),
    CONSTRAINT fk_fact_tiempo FOREIGN KEY (fk_tiempo) REFERENCES dim_tiempo(sk_tiempo)
);

-- 4. Batch Audit & Reconciliation Log
CREATE TABLE etl_batch_audit (
    batch_id VARCHAR(100) PRIMARY KEY,
    loaded_at_utc VARCHAR(60) NOT NULL,
    dim_tiempo_count INT NOT NULL,
    dim_geografia_count INT NOT NULL,
    fact_irca_count INT NOT NULL,
    avg_irca_general DECIMAL(8,4) NOT NULL
);

-- =================================================================================
-- PART 2: STREAMING METRICS STORE (KAFKA CONSUMER -> REAL-TIME DASHBOARD)
-- =================================================================================

-- 5. Validated Streaming Events Log (Idempotent by event_id)
CREATE TABLE IF NOT EXISTS stream_events_log (
    event_id VARCHAR(64) PRIMARY KEY COMMENT 'Unique UUIDv4 identifier per streaming event',
    event_timestamp VARCHAR(40) NOT NULL COMMENT 'UTC ISO-8601 generation timestamp',
    processed_at_utc VARCHAR(40) NOT NULL COMMENT 'UTC ISO-8601 consumer processing timestamp',
    event_key VARCHAR(20) NOT NULL COMMENT 'Kafka partition key (codigo_municipio)',
    event_type VARCHAR(40) NOT NULL COMMENT 'NEW_SAMPLE_MEASUREMENT or DELTA_ALERT_UPDATE',
    simulation_mode VARCHAR(40) NOT NULL COMMENT 'REPLAY_SIMULATION or LIVE_SURVEILLANCE',
    codigo_municipio VARCHAR(10) NOT NULL,
    municipio VARCHAR(150) NOT NULL,
    departamento VARCHAR(150) NOT NULL,
    region_geografica VARCHAR(100) NOT NULL,
    anio_referencia INT NOT NULL,
    irca_general DECIMAL(6,2) NOT NULL,
    irca_urbano DECIMAL(6,2) NULL,
    irca_rural DECIMAL(6,2) NULL,
    delta_irca_prev DECIMAL(6,2) NOT NULL COMMENT 'Change vs previous municipal IRCA reading',
    nivel_riesgo VARCHAR(50) NOT NULL,
    tiene_resguardo_indigena TINYINT(1) NOT NULL,
    total_resguardos_indigenas INT NOT NULL,
    poblacion_depto_api BIGINT NOT NULL,
    kafka_partition INT NOT NULL DEFAULT 0,
    kafka_offset BIGINT NOT NULL DEFAULT 0
);

-- 6. Real-Time Global Streaming KPIs Snapshot (Single-row continuously updated store + history)
CREATE TABLE IF NOT EXISTS stream_metrics_realtime (
    metric_snapshot_id INT AUTO_INCREMENT PRIMARY KEY,
    updated_at_utc VARCHAR(40) NOT NULL,
    total_valid_events INT NOT NULL COMMENT 'Metric 1a: Cumulative valid events processed',
    total_rejected_events INT NOT NULL COMMENT 'Metric 1b: Cumulative invalid events rejected by contract',
    rolling_avg_irca_general DECIMAL(6,2) NOT NULL COMMENT 'Metric 1: Rolling window average IRCA (last 50 events)',
    rolling_avg_irca_rural DECIMAL(6,2) NOT NULL COMMENT 'Metric 1c: Rolling window average Rural IRCA',
    critical_alerts_count INT NOT NULL COMMENT 'Metric 2: Active High Risk (>35%) & Sanitarily Inviable (>80%) alerts',
    inviable_sanitaria_count INT NOT NULL COMMENT 'Metric 2b: Extreme Sanitarily Inviable (>80%) alerts',
    indigenous_municipalities_alert_count INT NOT NULL COMMENT 'Metric 3a: Alerts in municipalities with Indigenous Reservations',
    indigenous_rolling_avg_irca DECIMAL(6,2) NOT NULL COMMENT 'Metric 3b: Live average IRCA in Indigenous territories',
    non_indigenous_rolling_avg_irca DECIMAL(6,2) NOT NULL COMMENT 'Metric 3c: Live average IRCA in Non-Indigenous territories',
    indigenous_gap_pts DECIMAL(6,2) NOT NULL COMMENT 'Metric 3: Live Indigenous vs Non-Indigenous IRCA gap (pts)',
    last_event_municipio VARCHAR(150) NOT NULL,
    last_event_departamento VARCHAR(150) NOT NULL,
    last_event_irca DECIMAL(6,2) NOT NULL,
    last_event_riesgo VARCHAR(50) NOT NULL
);

-- 7. Real-Time Regional Aggregation Store (Metric 4: Continuously updated per Natural Region)
CREATE TABLE IF NOT EXISTS stream_regional_metrics (
    region_geografica VARCHAR(100) PRIMARY KEY,
    events_processed INT NOT NULL,
    avg_irca_general DECIMAL(6,2) NOT NULL,
    avg_irca_rural DECIMAL(6,2) NULL,
    max_irca_recorded DECIMAL(6,2) NOT NULL,
    critical_alerts_count INT NOT NULL,
    indigenous_alerts_count INT NOT NULL,
    updated_at_utc VARCHAR(40) NOT NULL
);

-- 8. Dead-Letter Queue (DLQ) for Invalid Events Rejected by Consumer Contract Validation
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
);
