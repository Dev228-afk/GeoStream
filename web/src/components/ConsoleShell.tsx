import type { ReactNode } from 'react';
import { NavLink } from 'react-router-dom';
import { geoApi, type RunRecordDto } from '../api/client';
import { useHealth, useRunSummary } from '../api/hooks';

interface Props {
  title: string;
  subtitle: string;
  runs: RunRecordDto[];
  selectedRunId: string;
  onSelectRun: (runId: string) => void;
  children: ReactNode;
}

export function ConsoleShell({ title, subtitle, runs, selectedRunId, onSelectRun, children }: Props) {
  const healthQ = useHealth();
  const summaryQ = useRunSummary(selectedRunId);

  async function handleDownloadEvidence() {
    if (!selectedRunId) return;
    const [sum, rec] = await Promise.all([
      geoApi.getRunSummary(selectedRunId).catch(() => null),
      geoApi.getReconciliation(selectedRunId).catch(() => null),
    ]);
    const payload = JSON.stringify({ runId: selectedRunId, summary: sum, reconciliation: rec }, null, 2);
    const blob = new Blob([payload], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `geostream-${selectedRunId}-evidence.json`;
    a.click();
    URL.revokeObjectURL(url);
  }

  const mode = summaryQ.data?.mode ?? 'RECORDED';
  const freshness = summaryQ.data?.dataFreshnessAt
    ? new Date(summaryQ.data.dataFreshnessAt).toLocaleTimeString([], { hour12: false })
    : '—';

  const deps = healthQ.data?.dependencies;

  return (
    <div className="gs-layout">
      <aside className="gs-sidebar" aria-label="Primary navigation">
        <div className="gs-brand">
          <div className="gs-brand-mark" aria-hidden="true">GS</div>
          <div>
            <div style={{ fontWeight: 700, fontSize: '15px' }}>GeoStream</div>
            <div style={{ fontSize: '12px', color: 'var(--muted)' }}>Synthetic Telemetry Console</div>
          </div>
        </div>

        <nav className="gs-nav" aria-label="Console routes">
          <NavLink to="/overview" className={({ isActive }) => `gs-nav-link ${isActive ? 'active' : ''}`}>
            Overview
          </NavLink>
          <NavLink to="/map" className={({ isActive }) => `gs-nav-link ${isActive ? 'active' : ''}`}>
            Map &amp; replay
          </NavLink>
          <NavLink to="/diagnostics" className={({ isActive }) => `gs-nav-link ${isActive ? 'active' : ''}`}>
            Diagnostics
          </NavLink>
          <NavLink to="/transitions" className={({ isActive }) => `gs-nav-link ${isActive ? 'active' : ''}`}>
            Transitions
          </NavLink>
          <NavLink
            to={`/runs/${encodeURIComponent(selectedRunId || 'run-local-001')}/evidence`}
            className={({ isActive }) => `gs-nav-link ${isActive ? 'active' : ''}`}
          >
            Run evidence
          </NavLink>
        </nav>

        <div
          style={{
            marginTop: 'auto',
            padding: '12px',
            borderRadius: 'var(--r-sm)',
            background: 'var(--surface-muted)',
            fontSize: '12px',
            color: 'var(--muted)',
          }}
        >
          <strong style={{ color: 'var(--ink)' }}>Synthetic routes only</strong>
          <div style={{ marginTop: '4px' }}>
            Deterministic simulator coordinates. PG owns durable transitions; Redis caches latest state (24h TTL).
          </div>
        </div>
      </aside>

      <div className="gs-main-col">
        <header className="gs-topbar">
          <div className="gs-topbar-group">
            <label htmlFor="run-select" style={{ fontSize: '13px', fontWeight: 600, color: 'var(--muted)' }}>
              Run:
            </label>
            <select
              id="run-select"
              aria-label="Select telemetry run"
              className="gs-select"
              value={selectedRunId}
              onChange={(e) => onSelectRun(e.target.value)}
            >
              {runs.map((r) => (
                <option key={r.id} value={r.id}>
                  {r.id} ({r.profile})
                </option>
              ))}
            </select>
            <span className={`gs-badge ${mode === 'LIVE' ? 'gs-badge-accent' : 'gs-badge-muted'}`}>
              {mode === 'LIVE' ? 'LIVE STREAM' : 'RECORDED RUN'}
            </span>
          </div>

          <div className="gs-topbar-group">
            <span className="gs-badge gs-badge-muted gs-num" title="Last observed run timestamp">
              Freshness: {freshness}
            </span>
            {deps ? (
              <>
                <span className={`gs-badge ${deps.postgres === 'ok' ? 'gs-badge-success' : 'gs-badge-danger'}`}>
                  PG: {deps.postgres}
                </span>
                <span className={`gs-badge ${deps.redis === 'ok' ? 'gs-badge-success' : 'gs-badge-danger'}`}>
                  Redis: {deps.redis}
                </span>
                <span className={`gs-badge ${deps.prometheus === 'ok' ? 'gs-badge-success' : 'gs-badge-warn'}`}>
                  Prom: {deps.prometheus}
                </span>
              </>
            ) : null}
            <button type="button" className="gs-btn" onClick={() => void handleDownloadEvidence()}>
              Download evidence
            </button>
          </div>
        </header>

        <main className="gs-main">
          <div>
            <h1 className="gs-page-title">{title}</h1>
            <p className="gs-page-sub">{subtitle}</p>
          </div>
          {children}
        </main>
      </div>
    </div>
  );
}

