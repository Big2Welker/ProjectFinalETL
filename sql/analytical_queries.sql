-- =================================================================================
-- SCRIPT: BATCH (R1 TO R7) & STREAMING (S1 TO S4) ANALYTICAL QUERIES - FINAL DELIVERY
-- Database: dw_calidad_agua_colombia (MySQL 8.0)
-- =================================================================================

-- ---------------------------------------------------------------------------------
-- R1 (Refined): Historical national evolution of IRCA (General, Urban, Rural) 2007-2024.
-- ---------------------------------------------------------------------------------
SELECT 
    t.anio,
    COUNT(f.fk_geografia) AS municipios_evaluados,
    ROUND(AVG(f.irca_general), 2) AS prom_irca_general,
    ROUND(AVG(f.irca_urbano), 2) AS prom_irca_urbano,
    ROUND(AVG(f.irca_rural), 2) AS prom_irca_rural
FROM fact_irca f
JOIN dim_tiempo t ON f.fk_tiempo = t.sk_tiempo
GROUP BY t.anio
ORDER BY t.anio ASC;

-- ---------------------------------------------------------------------------------
-- R2 (Refined & Corrected): Top 5 departments with worst water quality in latest year.
-- Eliminates COALESCE(irca, 0) bias and uses official consolidated `irca_general`.
-- ---------------------------------------------------------------------------------
SELECT 
    g.departamento,
    g.region_geografica,
    COUNT(f.fk_geografia) AS municipios_medidos,
    ROUND(AVG(f.irca_general), 2) AS prom_irca_consolidado_real,
    ROUND(AVG(f.irca_urbano), 2) AS prom_irca_urbano_real,
    ROUND(AVG(f.irca_rural), 2) AS prom_irca_rural_real
FROM fact_irca f
JOIN dim_geografia g ON f.fk_geografia = g.sk_geografia
JOIN dim_tiempo t ON f.fk_tiempo = t.sk_tiempo
WHERE t.anio = (SELECT MAX(anio) FROM dim_tiempo)
GROUP BY g.departamento, g.region_geografica
ORDER BY prom_irca_consolidado_real DESC
LIMIT 5;

-- ---------------------------------------------------------------------------------
-- R3 (Refined): Rural vs. Urban inequality gap per department.
-- ---------------------------------------------------------------------------------
SELECT 
    g.departamento,
    ROUND(AVG(f.irca_urbano), 2) AS prom_urbano,
    ROUND(AVG(f.irca_rural), 2) AS prom_rural,
    ROUND(AVG(f.irca_rural) - AVG(f.irca_urbano), 2) AS brecha_departamental,
    ROUND(AVG(f.brecha_rural_urbana), 2) AS brecha_municipal_pareada
FROM fact_irca f
JOIN dim_geografia g ON f.fk_geografia = g.sk_geografia
GROUP BY g.departamento
HAVING prom_rural IS NOT NULL AND prom_urbano IS NOT NULL
ORDER BY brecha_departamental DESC;

-- ---------------------------------------------------------------------------------
-- R4 (Refined): Municipalities in chronic High Risk or Sanitarily Inviable (>35%)
-- across all 5 years of the recent lustrum (2020-2024).
-- ---------------------------------------------------------------------------------
SELECT 
    g.codigo_municipio,
    g.departamento,
    g.municipio,
    g.region_geografica,
    COUNT(t.anio) AS anios_consecutivos_alto_riesgo,
    ROUND(AVG(f.irca_general), 2) AS prom_irca_general_5a,
    ROUND(AVG(f.irca_rural), 2) AS prom_irca_rural_5a
FROM fact_irca f
JOIN dim_geografia g ON f.fk_geografia = g.sk_geografia
JOIN dim_tiempo t ON f.fk_tiempo = t.sk_tiempo
WHERE t.es_lustro_reciente = 1
  AND (f.irca_general > 35.0 OR f.irca_rural > 35.0)
GROUP BY g.codigo_municipio, g.departamento, g.municipio, g.region_geografica
HAVING COUNT(t.anio) = 5
ORDER BY prom_irca_general_5a DESC;

-- ---------------------------------------------------------------------------------
-- R5 (Maintained): Departments with greatest historical reduction in rural water risk.
-- ---------------------------------------------------------------------------------
WITH min_anio AS (
    SELECT g.departamento, AVG(f.irca_rural) AS irca_base
    FROM fact_irca f
    JOIN dim_geografia g ON f.fk_geografia = g.sk_geografia
    JOIN dim_tiempo t ON f.fk_tiempo = t.sk_tiempo
    WHERE t.anio = (SELECT MIN(anio) FROM dim_tiempo)
    GROUP BY g.departamento
),
max_anio AS (
    SELECT g.departamento, AVG(f.irca_rural) AS irca_actual
    FROM fact_irca f
    JOIN dim_geografia g ON f.fk_geografia = g.sk_geografia
    JOIN dim_tiempo t ON f.fk_tiempo = t.sk_tiempo
    WHERE t.anio = (SELECT MAX(anio) FROM dim_tiempo)
    GROUP BY g.departamento
)
SELECT 
    a.departamento,
    ROUND(a.irca_base, 2) AS riesgo_rural_base_2007,
    ROUND(b.irca_actual, 2) AS riesgo_rural_actual_2024,
    ROUND(b.irca_actual - a.irca_base, 2) AS reduccion_puntos_irca
FROM min_anio a
JOIN max_anio b ON a.departamento = b.departamento
WHERE a.irca_base IS NOT NULL 
  AND b.irca_actual IS NOT NULL
  AND (b.irca_actual - a.irca_base) < 0
ORDER BY reduccion_puntos_irca ASC;

-- ---------------------------------------------------------------------------------
-- R6 (Integrated CSV INS + API Colombia /Department & /Region):
-- Departmental Demographic Exposure to Water Risk and Population Density.
-- ---------------------------------------------------------------------------------
SELECT 
    g.region_geografica,
    g.departamento,
    MAX(g.poblacion_depto_api) AS poblacion_departamental_api,
    MAX(g.densidad_depto_hab_km2) AS densidad_hab_km2_api,
    COUNT(f.fk_geografia) AS total_municipios_evaluados,
    SUM(CASE WHEN f.irca_general > 14.0 THEN 1 ELSE 0 END) AS municipios_riesgo_medio_o_alto,
    ROUND(SUM(CASE WHEN f.irca_general > 14.0 THEN 1 ELSE 0 END) * 100.0 / COUNT(f.fk_geografia), 2) AS pct_municipios_afectados,
    ROUND(AVG(f.irca_general), 2) AS prom_irca_depto,
    ROUND(MAX(g.poblacion_depto_api) * (SUM(CASE WHEN f.irca_general > 14.0 THEN 1 ELSE 0 END) / COUNT(f.fk_geografia)), 0) AS poblacion_estimada_expuesta_riesgo
FROM fact_irca f
JOIN dim_geografia g ON f.fk_geografia = g.sk_geografia
JOIN dim_tiempo t ON f.fk_tiempo = t.sk_tiempo
WHERE t.anio = (SELECT MAX(anio) FROM dim_tiempo)
GROUP BY g.region_geografica, g.departamento
ORDER BY poblacion_estimada_expuesta_riesgo DESC;

-- ---------------------------------------------------------------------------------
-- R7 (Integrated CSV INS + API Colombia /IndigenousReservation):
-- Water Vulnerability Gap in Municipalities with Indigenous Reservations.
-- ---------------------------------------------------------------------------------
SELECT 
    CASE WHEN g.tiene_resguardo_indigena = 1 THEN 'Con Resguardo Indígena (API)' ELSE 'Sin Resguardo Indígena' END AS condicion_territorial,
    COUNT(DISTINCT g.sk_geografia) AS cantidad_municipios,
    SUM(g.total_resguardos_indigenas) / COUNT(DISTINCT t.anio) AS total_resguardos_asociados,
    ROUND(AVG(f.irca_general), 2) AS prom_irca_general_historico,
    ROUND(AVG(f.irca_rural), 2) AS prom_irca_rural_historico,
    ROUND(AVG(CASE WHEN t.anio = (SELECT MAX(anio) FROM dim_tiempo) THEN f.irca_general END), 2) AS prom_irca_general_2024,
    ROUND(AVG(CASE WHEN t.anio = (SELECT MAX(anio) FROM dim_tiempo) THEN f.irca_rural END), 2) AS prom_irca_rural_2024
FROM fact_irca f
JOIN dim_geografia g ON f.fk_geografia = g.sk_geografia
JOIN dim_tiempo t ON f.fk_tiempo = t.sk_tiempo
GROUP BY g.tiene_resguardo_indigena
ORDER BY g.tiene_resguardo_indigena DESC;

-- =================================================================================
-- PART 2: REAL-TIME STREAMING QUERIES (S1 TO S4 - KAFKA CONSUMER METRICS STORE)
-- =================================================================================

-- S1: Latest Real-Time Streaming KPI Snapshot (Rolling IRCA, Valid vs Rejected Events)
SELECT *
FROM stream_metrics_realtime
ORDER BY metric_snapshot_id DESC
LIMIT 1;

-- S2: Real-Time Sanitary Emergency & High-Risk Stream Alerts (IRCA > 35%)
SELECT 
    event_id,
    event_timestamp,
    codigo_municipio,
    municipio,
    departamento,
    region_geografica,
    irca_general,
    irca_rural,
    delta_irca_prev,
    nivel_riesgo,
    tiene_resguardo_indigena,
    total_resguardos_indigenas
FROM stream_events_log
WHERE irca_general > 35.0
ORDER BY processed_at_utc DESC
LIMIT 20;

-- S3: Real-Time Regional Streaming Aggregation
SELECT *
FROM stream_regional_metrics
ORDER BY avg_irca_general DESC;

-- S4: Dead-Letter Queue (DLQ) Audit of Invalid Streaming Events Rejected by Contract
SELECT *
FROM stream_dlq_invalid_events
ORDER BY dlq_id DESC
LIMIT 20;
