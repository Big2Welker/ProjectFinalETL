"""
Real-Time Streaming Dashboard Server & Live UI (Section 2.7).
- Connects directly to the MySQL Streaming Metrics Store (`dw_calidad_agua_colombia`).
- Exposes:
  * `GET /` -> Interactive Real-Time Dashboard (auto-refreshes every 2 seconds).
  * `GET /api/live-state` -> Live JSON state from `stream_metrics_realtime`, `stream_regional_metrics`,
    `stream_events_log`, and `stream_dlq_invalid_events`.
  * `POST /api/publish-wave` -> Triggers the Kafka Producer + Consumer to publish and process a new
    wave of municipal telemetry events so the user/professor can watch the dashboard update live.
  * `POST /api/inject-invalid` -> Injects an invalid event into Kafka to demonstrate live DLQ rejection.
"""

import os
import sys
import json
import random
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from streaming.kafka_producer import IRCATelemetryProducer, fetch_dw_records_for_replay
from streaming.kafka_consumer import IRCATelemetryConsumer, get_db_connection
from streaming.run_streaming_experiments import fetch_dashboard_state_from_mysql

LIVE_GROUP_ID = "cg_irca_live_dashboard_v1"
_offset_cursor = 120


def get_full_live_payload() -> dict:
    state = fetch_dashboard_state_from_mysql()
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT metric_snapshot_id, updated_at_utc, total_valid_events,
                       rolling_avg_irca_general, rolling_avg_irca_rural,
                       indigenous_rolling_avg_irca, non_indigenous_rolling_avg_irca,
                       critical_alerts_count
                FROM stream_metrics_realtime
                ORDER BY metric_snapshot_id DESC
                LIMIT 25;
            """)
            history = list(reversed(cur.fetchall()))
            for h in history:
                for k, v in list(h.items()):
                    if hasattr(v, "__float__") and not isinstance(v, (int, bool)):
                        h[k] = float(v)
            state["metrics_history"] = history
    finally:
        conn.close()
    return state


DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Real-Time Streaming Dashboard · Kafka + MySQL (ODS 6 IRCA Colombia)</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
  <style>
    :root {
      --bg: #0F1117;
      --card: #171A24;
      --card-alt: #1E2330;
      --border: #2C3347;
      --text: #F8FAFC;
      --muted: #94A3B8;
      --ruby: #F54651;
      --peach: #F2A7A0;
      --cyan: #38BDF8;
      --emerald: #10B981;
      --amber: #F59E0B;
      --purple: #A855F7;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      background: var(--bg);
      color: var(--text);
      font-family: 'Segoe UI', Inter, system-ui, sans-serif;
      padding: 18px 26px;
    }
    .header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      border-bottom: 1px solid var(--border);
      padding-bottom: 14px;
      margin-bottom: 18px;
    }
    .title-wrap h1 {
      font-size: 22px;
      font-weight: 800;
      display: flex;
      align-items: center;
      gap: 10px;
    }
    .live-dot {
      width: 11px;
      height: 11px;
      background: var(--emerald);
      border-radius: 50%;
      display: inline-block;
      box-shadow: 0 0 12px var(--emerald);
      animation: pulse 1.6s infinite;
    }
    @keyframes pulse {
      0%, 100% { transform: scale(1); opacity: 1; }
      50% { transform: scale(1.25); opacity: 0.65; }
    }
    .subtitle { font-size: 12.5px; color: var(--muted); margin-top: 4px; }
    .actions { display: flex; gap: 10px; align-items: center; }
    .btn {
      background: var(--card-alt);
      color: var(--text);
      border: 1px solid var(--border);
      padding: 8px 14px;
      border-radius: 8px;
      font-size: 12.5px;
      font-weight: 700;
      cursor: pointer;
      transition: all 0.15s;
    }
    .btn:hover { border-color: var(--cyan); }
    .btn-ruby { background: var(--ruby); border-color: var(--ruby); color: #fff; }
    .btn-ruby:hover { background: #dc2626; }
    .kpi-grid {
      display: grid;
      grid-template-columns: repeat(6, 1fr);
      gap: 12px;
      margin-bottom: 16px;
    }
    .kpi {
      background: var(--card);
      border: 1px solid var(--border);
      border-top: 3px solid var(--ruby);
      border-radius: 12px;
      padding: 12px 14px;
    }
    .kpi-label { font-size: 11px; color: var(--muted); font-weight: 700; text-transform: uppercase; }
    .kpi-val { font-size: 24px; font-weight: 900; margin-top: 4px; }
    .kpi-sub { font-size: 11px; color: var(--peach); margin-top: 3px; }
    .charts-grid {
      display: grid;
      grid-template-columns: 1.35fr 1fr 1fr;
      gap: 14px;
      margin-bottom: 16px;
    }
    .panel {
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 14px;
      padding: 14px 16px;
      display: flex;
      flex-direction: column;
      gap: 10px;
    }
    .panel-title {
      font-size: 13.5px;
      font-weight: 800;
      display: flex;
      justify-content: space-between;
      align-items: center;
    }
    .badge {
      font-size: 10.5px;
      padding: 2px 8px;
      border-radius: 99px;
      background: rgba(56, 189, 248, 0.15);
      color: var(--cyan);
      border: 1px solid rgba(56, 189, 248, 0.35);
    }
    .chart-container { height: 230px; position: relative; }
    .tables-grid {
      display: grid;
      grid-template-columns: 1.35fr 1fr;
      gap: 14px;
    }
    table { width: 100%; border-collapse: collapse; font-size: 11.5px; }
    th {
      text-align: left;
      padding: 7px 8px;
      background: var(--card-alt);
      color: var(--peach);
      border-bottom: 2px solid var(--ruby);
    }
    td { padding: 6px 8px; border-bottom: 1px solid var(--border); color: #E2E8F0; }
  </style>
</head>
<body>
  <div class="header">
    <div class="title-wrap">
      <h1>
        <span class="live-dot"></span>
        Real-Time Water Quality (IRCA) Streaming Dashboard · Apache Kafka → MySQL Store
      </h1>
      <div class="subtitle">
        Topic: <code>irca.water.telemetry.v1</code> · Auto-Refresh: <strong>Every 2.0s</strong> ·
        Last Update: <span id="lastUpdate">Cargando...</span>
      </div>
    </div>
    <div class="actions">
      <button class="btn btn-ruby" onclick="triggerWave()">⚡ Publicar Nueva Ola en Kafka (+15 Eventos)</button>
      <button class="btn" onclick="triggerInvalid()">🛑 Inyectar Evento Corrupto (Probar DLQ)</button>
    </div>
  </div>

  <!-- 6 Live Streaming Metrics KPIs -->
  <div class="kpi-grid">
    <div class="kpi" style="border-top-color: var(--emerald);">
      <div class="kpi-label">Métrica S1A · Eventos Válidos</div>
      <div class="kpi-val" id="kpiValid" style="color: var(--emerald);">0</div>
      <div class="kpi-sub" id="kpiLastMpio">Último: -</div>
    </div>
    <div class="kpi" style="border-top-color: var(--ruby);">
      <div class="kpi-label">Métrica S1B · Rolling IRCA (50 ev)</div>
      <div class="kpi-val" id="kpiRollingGen" style="color: var(--ruby);">0.00%</div>
      <div class="kpi-sub" id="kpiRollingRur">Rural Rolling: 0.00%</div>
    </div>
    <div class="kpi" style="border-top-color: var(--amber);">
      <div class="kpi-label">Métrica S2 · Alertas Críticas (&gt;35%)</div>
      <div class="kpi-val" id="kpiCritAlerts" style="color: var(--amber);">0</div>
      <div class="kpi-sub" id="kpiInviable">Inviable Sanitaria (&gt;80%): 0</div>
    </div>
    <div class="kpi" style="border-top-color: var(--cyan);">
      <div class="kpi-label">Métrica S3A · IRCA Territorios Indígenas</div>
      <div class="kpi-val" id="kpiIndAvg" style="color: var(--cyan);">0.00%</div>
      <div class="kpi-sub" id="kpiIndAlerts">Alertas en Resguardos: 0</div>
    </div>
    <div class="kpi" style="border-top-color: var(--purple);">
      <div class="kpi-label">Métrica S3B · Brecha Indígena Live</div>
      <div class="kpi-val" id="kpiIndGap" style="color: var(--purple);">+0.00 pts</div>
      <div class="kpi-sub" id="kpiNonIndAvg">Sin Resguardo: 0.00%</div>
    </div>
    <div class="kpi" style="border-top-color: #EF4444;">
      <div class="kpi-label">Contrato · Eventos Rechazados (DLQ)</div>
      <div class="kpi-val" id="kpiRejected" style="color: #EF4444;">0</div>
      <div class="kpi-sub">Bloqueados por Event Contract</div>
    </div>
  </div>

  <!-- 3 Real-Time Streaming Charts -->
  <div class="charts-grid">
    <div class="panel">
      <div class="panel-title">
        <span>📈 Evolución en Tiempo Real del Rolling IRCA (General, Rural e Indígena)</span>
        <span class="badge">Ventana Deslizante (S1)</span>
      </div>
      <div class="chart-container"><canvas id="chartStreamLine"></canvas></div>
    </div>
    <div class="panel">
      <div class="panel-title">
        <span>🗺️ Métrica S4 · Riesgo Promedio en Vivo por Región Natural</span>
        <span class="badge">stream_regional_metrics</span>
      </div>
      <div class="chart-container"><canvas id="chartRegionBar"></canvas></div>
    </div>
    <div class="panel">
      <div class="panel-title">
        <span>⛺ Métrica S3 · Comparativa Live Con vs. Sin Resguardo Indígena</span>
        <span class="badge">API + Stream</span>
      </div>
      <div class="chart-container"><canvas id="chartIndBar"></canvas></div>
    </div>
  </div>

  <!-- 2 Live Tables: Recent Validated Stream Events & Dead-Letter Queue (DLQ) -->
  <div class="tables-grid">
    <div class="panel">
      <div class="panel-title">
        <span>📡 Últimos Eventos Procesados por el Kafka Consumer (stream_events_log)</span>
        <span class="badge">Key: codigo_municipio</span>
      </div>
      <table>
        <thead>
          <tr>
            <th>Offset</th>
            <th>Mpio (Cód)</th>
            <th>Depto / Región</th>
            <th>IRCA (%)</th>
            <th>Δ Previo</th>
            <th>Resguardos</th>
            <th>Nivel de Riesgo</th>
          </tr>
        </thead>
        <tbody id="tbodyEvents"></tbody>
      </table>
    </div>

    <div class="panel">
      <div class="panel-title">
        <span>🛡️ Dead-Letter Queue: Eventos Inválidos Rechazados (stream_dlq_invalid_events)</span>
        <span class="badge" style="color:var(--ruby); border-color:var(--ruby);">Contract Enforcement</span>
      </div>
      <table>
        <thead>
          <tr>
            <th>ID</th>
            <th>Código Violación</th>
            <th>Detalle del Contrato</th>
            <th>Part/Offset</th>
          </tr>
        </thead>
        <tbody id="tbodyDlq"></tbody>
      </table>
    </div>
  </div>

  <script>
    Chart.defaults.color = '#94A3B8';
    Chart.defaults.borderColor = 'rgba(148, 163, 184, 0.15)';

    const lineChart = new Chart(document.getElementById('chartStreamLine'), {
      type: 'line',
      data: {
        labels: [],
        datasets: [
          { label: 'Rolling IRCA General (%)', data: [], borderColor: '#F54651', borderWidth: 2.5, tension: 0.3 },
          { label: 'Rolling IRCA Rural (%)', data: [], borderColor: '#F2A7A0', borderWidth: 2, tension: 0.3 },
          { label: 'IRCA Municipios Indígenas (%)', data: [], borderColor: '#38BDF8', borderWidth: 2, tension: 0.3 }
        ]
      },
      options: { responsive: true, maintainAspectRatio: false, animation: false }
    });

    const regionChart = new Chart(document.getElementById('chartRegionBar'), {
      type: 'bar',
      data: {
        labels: [],
        datasets: [
          { label: 'IRCA Promedio Live (%)', data: [], backgroundColor: '#F54651', borderRadius: 4 },
          { label: 'Alertas Críticas (>35%)', data: [], backgroundColor: '#F59E0B', borderRadius: 4 }
        ]
      },
      options: { indexAxis: 'y', responsive: true, maintainAspectRatio: false, animation: false }
    });

    const indChart = new Chart(document.getElementById('chartIndBar'), {
      type: 'bar',
      data: {
        labels: ['Con Resguardo Indígena', 'Sin Resguardo Indígena'],
        datasets: [{
          label: 'IRCA Promedio en Streaming (%)',
          data: [0, 0],
          backgroundColor: ['#F54651', '#38BDF8'],
          borderRadius: 6
        }]
      },
      options: { responsive: true, maintainAspectRatio: false, animation: false, scales: { y: { beginAtZero: true } } }
    });

    function renderDashboard(data) {
      const k = data.kpi_snapshot || {};
      document.getElementById('lastUpdate').textContent = k.updated_at_utc || data.captured_at_utc || '-';
      document.getElementById('kpiValid').textContent = k.total_valid_events || 0;
      document.getElementById('kpiLastMpio').textContent = `Último: ${k.last_event_municipio || '-'} (${k.last_event_irca || 0}%)`;
      document.getElementById('kpiRollingGen').textContent = `${(k.rolling_avg_irca_general || 0).toFixed(2)}%`;
      document.getElementById('kpiRollingRur').textContent = `Rural Rolling: ${(k.rolling_avg_irca_rural || 0).toFixed(2)}%`;
      document.getElementById('kpiCritAlerts').textContent = k.critical_alerts_count || 0;
      document.getElementById('kpiInviable').textContent = `Inviable Sanitaria (>80%): ${k.inviable_sanitaria_count || 0}`;
      document.getElementById('kpiIndAvg').textContent = `${(k.indigenous_rolling_avg_irca || 0).toFixed(2)}%`;
      document.getElementById('kpiIndAlerts').textContent = `Alertas en Resguardos: ${k.indigenous_municipalities_alert_count || 0}`;
      const gap = k.indigenous_gap_pts || 0;
      document.getElementById('kpiIndGap').textContent = `${gap >= 0 ? '+' : ''}${gap.toFixed(2)} pts`;
      document.getElementById('kpiNonIndAvg').textContent = `Sin Resguardo: ${(k.non_indigenous_rolling_avg_irca || 0).toFixed(2)}%`;
      document.getElementById('kpiRejected').textContent = k.total_rejected_events || 0;

      // Update Line Chart
      const hist = data.metrics_history || [];
      lineChart.data.labels = hist.map(h => `Ev #${h.total_valid_events}`);
      lineChart.data.datasets[0].data = hist.map(h => h.rolling_avg_irca_general);
      lineChart.data.datasets[1].data = hist.map(h => h.rolling_avg_irca_rural);
      lineChart.data.datasets[2].data = hist.map(h => h.indigenous_rolling_avg_irca);
      lineChart.update();

      // Update Region Chart
      const regs = data.regional_metrics || [];
      regionChart.data.labels = regs.map(r => r.region_geografica);
      regionChart.data.datasets[0].data = regs.map(r => r.avg_irca_general);
      regionChart.data.datasets[1].data = regs.map(r => r.critical_alerts_count);
      regionChart.update();

      // Update Indigenous Chart
      indChart.data.datasets[0].data = [k.indigenous_rolling_avg_irca || 0, k.non_indigenous_rolling_avg_irca || 0];
      indChart.update();

      // Update Events Table
      const evs = data.recent_events || [];
      document.getElementById('tbodyEvents').innerHTML = evs.slice(0, 8).map(e => `
        <tr>
          <td>P${e.kafka_partition}:${e.kafka_offset}</td>
          <td><strong>${e.municipio}</strong> (${e.codigo_municipio})</td>
          <td>${e.departamento} · <span style="color:#38BDF8">${e.region_geografica}</span></td>
          <td style="color:${e.irca_general > 35 ? '#F54651' : '#10B981'}; font-weight:700;">${Number(e.irca_general).toFixed(2)}%</td>
          <td>${Number(e.delta_irca_prev) >= 0 ? '+' : ''}${Number(e.delta_irca_prev).toFixed(2)}</td>
          <td>${e.tiene_resguardo_indigena ? '⛺ Sí (' + e.total_resguardos_indigenas + ')' : 'No'}</td>
          <td>${e.nivel_riesgo}</td>
        </tr>
      `).join('');

      // Update DLQ Table
      const dlq = data.dlq_invalid_events || [];
      document.getElementById('tbodyDlq').innerHTML = dlq.slice(-6).reverse().map(d => `
        <tr>
          <td>#${d.dlq_id}</td>
          <td style="color:#F54651; font-weight:700;">${d.violation_code}</td>
          <td>${d.violation_detail}</td>
          <td>P${d.kafka_partition}:${d.kafka_offset}</td>
        </tr>
      `).join('');
    }

    async function fetchLiveState() {
      if (window.__STATIC_SNAPSHOT_DATA__) {
        renderDashboard(window.__STATIC_SNAPSHOT_DATA__);
        return;
      }
      try {
        const res = await fetch('/api/live-state');
        const data = await res.json();
        renderDashboard(data);
      } catch (e) {
        console.error(e);
      }
    }

    async function triggerWave() {
      await fetch('/api/publish-wave', { method: 'POST' });
      await fetchLiveState();
    }

    async function triggerInvalid() {
      await fetch('/api/inject-invalid', { method: 'POST' });
      await fetchLiveState();
    }

    fetchLiveState();
    setInterval(fetchLiveState, 2000);
  </script>
</body>
</html>
"""


class DashboardRequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/live-state":
            payload = get_full_live_payload()
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(body)
        else:
            body = DASHBOARD_HTML.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(body)

    def do_POST(self):
        global _offset_cursor
        parsed = urlparse(self.path)
        if parsed.path == "/api/publish-wave":
            prod = IRCATelemetryProducer(min_delta_threshold=0.5)
            rows = fetch_dw_records_for_replay(limit=15, offset=_offset_cursor)
            _offset_cursor = (_offset_cursor + 15) % 1500
            for r in rows:
                prod.build_and_publish_record(r, delay_sec=0.0)
            prod.close()

            cons = IRCATelemetryConsumer(group_id=LIVE_GROUP_ID, auto_offset_reset="earliest", reset_store=False)
            cons.consume_batch(max_messages=30)
            cons.close()

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"status":"ok"}')
        elif parsed.path == "/api/inject-invalid":
            prod = IRCATelemetryProducer()
            prod.publish_invalid_test_events()
            prod.close()

            cons = IRCATelemetryConsumer(group_id=LIVE_GROUP_ID, auto_offset_reset="earliest", reset_store=False)
            cons.consume_batch(max_messages=10)
            cons.close()

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"status":"invalid_injected"}')

    def log_message(self, format, *args):
        return


def export_static_dashboard_snapshot() -> str:
    """Exports a self-contained HTML snapshot of the Real-Time Dashboard with current MySQL state."""
    payload = get_full_live_payload()
    injected_script = f"<script>window.__STATIC_SNAPSHOT_DATA__ = {json.dumps(payload, ensure_ascii=False)};</script>\n</head>"
    html_out = DASHBOARD_HTML.replace("</head>", injected_script)
    out_path = os.path.join(PROJECT_ROOT, "docs", "dashboard", "realtime_dashboard_snapshot.html")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html_out)
    return out_path


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--export-only", action="store_true", help="Export static HTML snapshot and exit")
    parser.add_argument("--port", type=int, default=8050, help="Port for live dashboard server")
    args = parser.parse_args()

    snap_file = export_static_dashboard_snapshot()
    print(f"[RealTimeDashboard] Exported snapshot to: {snap_file}")
    if not args.export_only:
        print(f"[RealTimeDashboard] Live Server running at http://localhost:{args.port}")
        server = HTTPServer(("0.0.0.0", args.port), DashboardRequestHandler)
        server.serve_forever()
