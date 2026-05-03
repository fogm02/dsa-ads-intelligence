"""
Build pipeline status HTML from live Azure Blob data.
Usage: python build_status.py
Output: test_output/status.html
"""

import json
import os
import sys

# Load env vars from local.settings.json
settings_path = os.path.join(os.path.dirname(__file__), "local.settings.json")
with open(settings_path) as f:
    settings = json.load(f)
for k, v in settings.get("Values", {}).items():
    os.environ.setdefault(k, v)

from shared.blob_helpers import get_container_client
from shared.status_collector import collect_pipeline_status


def main():
    print("Connecting to Azure Blob Storage...")
    container = get_container_client()

    print("Collecting pipeline status...")
    status = collect_pipeline_status(container)

    # Print summary to console
    silver = status["silver_summary"]
    health = status["health"]
    print(f"\nHealth: {health['status'].upper()}")
    print(f"Total ads in silver: {silver['total_ads']:,}")
    for p, count in silver["by_platform"].items():
        coverage = silver["advertisers_with_data"][p]
        print(f"  {p}: {count:,} ads ({coverage} advertisers with data)")

    if health["issues"]:
        print(f"\nIssues ({len(health['issues'])}):")
        for issue in health["issues"]:
            print(f"  - {issue}")

    # Generate HTML
    html = build_html(status)
    out_dir = os.path.join(os.path.dirname(__file__), "test_output")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "status.html")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"\nDashboard saved to: {out_path}")


STATUS_HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="cs">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Pipeline Status</title>
<style>
  :root {
    --bg: #0f172a; --surface: #1e293b; --surface2: #334155;
    --text: #e2e8f0; --text2: #94a3b8; --border: #475569;
    --ok: #22c55e; --warn: #f59e0b; --err: #ef4444;
    --tiktok: #ff6384; --meta: #36a2eb; --linkedin: #4bc0c0;
    --missing: #64748b;
  }
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
         background: var(--bg); color: var(--text); padding: 24px; line-height: 1.5; }

  .header { display: flex; align-items: center; gap: 16px; margin-bottom: 24px; }
  .header h1 { font-size: 1.5rem; }
  .health-badge { padding: 4px 12px; border-radius: 12px; font-size: 0.85rem; font-weight: 600; }
  .health-healthy { background: var(--ok); color: #000; }
  .health-warning { background: var(--warn); color: #000; }
  .health-error { background: var(--err); color: #fff; }
  .timestamp { color: var(--text2); font-size: 0.85rem; margin-left: auto; }

  .kpi-row { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
             gap: 12px; margin-bottom: 24px; }
  .kpi { background: var(--surface); border-radius: 8px; padding: 16px;
         border: 1px solid var(--border); }
  .kpi-value { font-size: 1.6rem; font-weight: 700; }
  .kpi-label { color: var(--text2); font-size: 0.8rem; margin-top: 4px; }
  .kpi-sub { color: var(--text2); font-size: 0.75rem; margin-top: 2px; }

  .platform-cards { display: grid; grid-template-columns: repeat(3, 1fr);
                    gap: 12px; margin-bottom: 24px; }
  .platform-card { background: var(--surface); border-radius: 8px; padding: 16px;
                   border-left: 4px solid; }
  .platform-card.tiktok { border-color: var(--tiktok); }
  .platform-card.meta { border-color: var(--meta); }
  .platform-card.linkedin { border-color: var(--linkedin); }
  .platform-card h3 { font-size: 1rem; margin-bottom: 8px; }
  .platform-card .stat { font-size: 0.85rem; color: var(--text2); margin: 2px 0; }
  .platform-card .stat b { color: var(--text); }

  .issues { background: var(--surface); border-radius: 8px; padding: 16px;
            margin-bottom: 24px; border: 1px solid var(--border); }
  .issues h3 { margin-bottom: 8px; }
  .issue-item { font-size: 0.85rem; color: var(--warn); padding: 2px 0; }
  .no-issues { color: var(--ok); font-size: 0.85rem; }

  .sector { background: var(--surface); border-radius: 8px; margin-bottom: 16px;
            border: 1px solid var(--border); overflow: hidden; }
  .sector-header { padding: 12px 16px; cursor: pointer; display: flex;
                   align-items: center; gap: 8px; user-select: none; }
  .sector-header:hover { background: var(--surface2); }
  .sector-header h3 { font-size: 0.95rem; flex: 1; }
  .sector-summary { font-size: 0.8rem; color: var(--text2); }
  .sector-arrow { transition: transform 0.2s; color: var(--text2); }
  .sector.open .sector-arrow { transform: rotate(90deg); }
  .sector-body { display: none; padding: 0 16px 12px; }
  .sector.open .sector-body { display: block; }

  .trend-panel { background: var(--surface); border-radius: 8px; padding: 16px;
                 margin-bottom: 24px; border: 1px solid var(--border); }
  .trend-panel h3 { margin-bottom: 12px; }
  .trend-canvas { width: 100%; max-height: 300px; }

  table { width: 100%; border-collapse: collapse; font-size: 0.85rem; }
  th { text-align: left; padding: 6px 8px; color: var(--text2); font-weight: 500;
       border-bottom: 1px solid var(--border); font-size: 0.8rem; }
  td { padding: 6px 8px; border-bottom: 1px solid rgba(71,85,105,0.3); }
  tr:last-child td { border-bottom: none; }

  .status-cell { text-align: center; min-width: 90px; }
  .st-ok { color: var(--ok); }
  .st-progress { color: var(--warn); }
  .st-missing { color: var(--missing); }
  .st-na { color: var(--border); }
  .st-error { color: var(--err); }

  @media (max-width: 800px) {
    .platform-cards { grid-template-columns: 1fr; }
    .kpi-row { grid-template-columns: repeat(2, 1fr); }
  }
</style>
</head>
<body>

<div class="header">
  <h1>Pipeline Status</h1>
  <span id="healthBadge" class="health-badge"></span>
  <span class="timestamp" id="timestamp"></span>
</div>

<div class="kpi-row" id="kpiRow"></div>
<div class="platform-cards" id="platformCards"></div>
<div class="trend-panel" id="trendPanel">
  <h3>Gold vyvoj v case</h3>
  <canvas id="trendChart" class="trend-canvas"></canvas>
</div>
<div class="issues" id="issuesPanel"></div>
<div id="sectors"></div>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.4/dist/chart.umd.min.js"></script>

<script>
const D = /*__DATA__*/null;

function init() {
  if (!D) { document.body.innerHTML = '<p>No data</p>'; return; }

  // Header
  const hb = document.getElementById('healthBadge');
  hb.textContent = D.health.status;
  hb.className = 'health-badge health-' + D.health.status;
  document.getElementById('timestamp').textContent =
    'Generated: ' + new Date(D.generated_at).toLocaleString('cs-CZ');

  // KPIs
  const s = D.silver_summary;
  const kpis = [
    { value: fmt(s.total_ads), label: 'Celkem reklam v silveru' },
    { value: fmt(s.by_platform.tiktok), label: 'TikTok reklam',
      sub: s.advertisers_with_data.tiktok + ' advertiserů' },
    { value: fmt(s.by_platform.meta), label: 'Meta reklam',
      sub: s.advertisers_with_data.meta + ' advertiserů' },
    { value: fmt(s.by_platform.linkedin), label: 'LinkedIn reklam',
      sub: s.advertisers_with_data.linkedin + ' advertiserů' },
    { value: Object.keys(D.sectors).length, label: 'Sektorů' },
  ];
  document.getElementById('kpiRow').innerHTML = kpis.map(k =>
    `<div class="kpi"><div class="kpi-value">${k.value}</div>` +
    `<div class="kpi-label">${k.label}</div>` +
    (k.sub ? `<div class="kpi-sub">${k.sub}</div>` : '') + '</div>'
  ).join('');

  // Platform cards
  const platforms = [
    { key: 'tiktok', name: 'TikTok', cls: 'tiktok' },
    { key: 'meta', name: 'Meta', cls: 'meta' },
    { key: 'linkedin', name: 'LinkedIn', cls: 'linkedin' },
  ];
  document.getElementById('platformCards').innerHTML = platforms.map(p => {
    const pd = D.platforms[p.key];
    let stats = `<div class="stat">Cycle: <b>${pd.cycle_id || '—'}</b></div>`;
    stats += `<div class="stat">Silver: <b>${fmt(pd.silver_total)}</b> reklam</div>`;
    stats += `<div class="stat">Config: <b>${pd.config_total}</b> advertiserů</div>`;
    if (pd.total_runs !== undefined)
      stats += `<div class="stat">Runs: <b>${pd.total_runs}</b></div>`;
    if (pd.total_ads_discovered !== undefined)
      stats += `<div class="stat">Discovered: <b>${fmt(pd.total_ads_discovered)}</b></div>`;
    if (pd.total_ads_enriched !== undefined)
      stats += `<div class="stat">Enriched: <b>${fmt(pd.total_ads_enriched)}</b></div>`;
    return `<div class="platform-card ${p.cls}"><h3>${p.name}</h3>${stats}</div>`;
  }).join('');

  // Issues
  const ip = document.getElementById('issuesPanel');
  if (D.health.issues.length === 0) {
    ip.innerHTML = '<h3>Issues</h3><div class="no-issues">Zadne issues</div>';
  } else {
    ip.innerHTML = '<h3>Issues (' + D.health.issues.length + ')</h3>' +
      D.health.issues.map(i => `<div class="issue-item">&#x26A0; ${i}</div>`).join('');
  }

  // Sectors
  const sectorsEl = document.getElementById('sectors');
  const sectorKeys = Object.keys(D.sectors).sort();
  sectorsEl.innerHTML = sectorKeys.map(sk => {
    const sec = D.sectors[sk];
    const advs = sec.advertisers || [];

    // Count statuses across platforms
    let okCount = 0, totalCells = 0;
    advs.forEach(a => {
      ['tiktok', 'meta', 'linkedin'].forEach(p => {
        if (a[p]) { totalCells++; if (a[p].status === 'ok') okCount++; }
      });
    });

    const rows = advs.map(a => {
      const tk = cellHtml(a.tiktok);
      const mt = cellHtml(a.meta);
      const li = cellHtml(a.linkedin);
      return `<tr><td>${a.name}</td><td class="status-cell">${tk}</td>` +
             `<td class="status-cell">${mt}</td><td class="status-cell">${li}</td></tr>`;
    }).join('');

    return `<div class="sector" id="sec-${sk}">` +
      `<div class="sector-header" onclick="toggleSector('${sk}')">` +
      `<span class="sector-arrow">&#9654;</span>` +
      `<h3>${sec.display_name}</h3>` +
      `<span class="sector-summary">${advs.length} advertiserů | ${okCount}/${totalCells} OK</span>` +
      `</div><div class="sector-body"><table>` +
      `<tr><th>Advertiser</th><th style="text-align:center">TikTok</th>` +
      `<th style="text-align:center">Meta</th><th style="text-align:center">LinkedIn</th></tr>` +
      rows + `</table></div></div>`;
  }).join('');

  // Auto-open sectors with issues
  sectorKeys.forEach(sk => {
    const sec = D.sectors[sk];
    const hasIssue = (sec.advertisers || []).some(a =>
      ['tiktok','meta','linkedin'].some(p => a[p] && a[p].status !== 'ok' && a[p].count === 0)
    );
    if (hasIssue) toggleSector(sk);
  });

  // Trend chart
  renderTrend();
}

function renderTrend() {
  const t = D.trend;
  if (!t || !t.dates || t.dates.length === 0) {
    document.getElementById('trendPanel').style.display = 'none';
    return;
  }

  const ctx = document.getElementById('trendChart').getContext('2d');
  const datasets = [];

  if (t.meta && t.meta.some(v => v > 0)) {
    datasets.push({
      label: 'Meta',
      data: t.meta,
      borderColor: '#36a2eb',
      backgroundColor: 'rgba(54,162,235,0.1)',
      fill: true, tension: 0.3, pointRadius: 4,
    });
  }
  if (t.tiktok && t.tiktok.some(v => v > 0)) {
    datasets.push({
      label: 'TikTok',
      data: t.tiktok,
      borderColor: '#ff6384',
      backgroundColor: 'rgba(255,99,132,0.1)',
      fill: true, tension: 0.3, pointRadius: 4,
    });
  }
  if (t.linkedin && t.linkedin.some(v => v > 0)) {
    datasets.push({
      label: 'LinkedIn',
      data: t.linkedin,
      borderColor: '#4bc0c0',
      backgroundColor: 'rgba(75,192,192,0.1)',
      fill: true, tension: 0.3, pointRadius: 4,
    });
  }
  if (t.total && t.total.some(v => v > 0)) {
    datasets.push({
      label: 'Celkem',
      data: t.total,
      borderColor: '#a78bfa',
      backgroundColor: 'rgba(167,139,250,0.1)',
      fill: false, tension: 0.3, pointRadius: 4,
      borderDash: [5, 5],
    });
  }

  new Chart(ctx, {
    type: 'line',
    data: { labels: t.dates, datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { labels: { color: '#e2e8f0' } },
        tooltip: {
          callbacks: {
            label: function(c) { return c.dataset.label + ': ' + fmt(c.raw) + ' reklam'; }
          }
        }
      },
      scales: {
        x: { ticks: { color: '#94a3b8' }, grid: { color: 'rgba(71,85,105,0.3)' } },
        y: {
          ticks: { color: '#94a3b8', callback: v => fmt(v) },
          grid: { color: 'rgba(71,85,105,0.3)' },
          beginAtZero: false,
        }
      }
    }
  });
}

function cellHtml(data) {
  if (!data) return '<span class="st-na">—</span>';
  if (data.status === 'ok') return `<span class="st-ok">&#x2705; ${fmt(data.count)}</span>`;
  if (data.status === 'collected' || data.status === 'discovered')
    return `<span class="st-progress">&#x23F3; ${data.status}</span>`;
  if (data.status === 'in_progress' || data.status === 'enriching')
    return `<span class="st-progress">&#x23F3; ${data.status}</span>`;
  if (data.status === 'error')
    return `<span class="st-error">&#x1F534; error</span>`;
  return `<span class="st-missing">&#x274C; 0</span>`;
}

function toggleSector(sk) {
  document.getElementById('sec-' + sk).classList.toggle('open');
}

function fmt(n) {
  if (n === null || n === undefined) return '0';
  return Number(n).toLocaleString('cs-CZ');
}

init();
</script>
</body>
</html>"""


def build_html(status_data):
    """Inject status data JSON into HTML template."""
    data_json = json.dumps(status_data, ensure_ascii=False, default=str)
    return STATUS_HTML_TEMPLATE.replace(
        "/*__DATA__*/null",
        json.dumps(status_data, ensure_ascii=False, default=str)
    )


if __name__ == "__main__":
    main()
