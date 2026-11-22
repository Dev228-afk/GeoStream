import { useState, type ReactNode } from 'react';
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

function GeoStreamLogo() {
  return (
    <svg width="32" height="32" viewBox="0 0 32 32" aria-hidden="true" style={{ flexShrink: 0 }}>
      <rect width="32" height="32" rx="9" fill="#0369a1" />
      <rect x="0.75" y="0.75" width="30.5" height="30.5" rx="8.25" fill="none" stroke="rgba(125,211,252,0.45)" />
      <path
        d="M8 21 L13 14 L18 17 L24 9"
        fill="none"
        stroke="#ffffff"
        strokeWidth="2.2"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
      <rect x="21.5" y="7" width="4.5" height="4.5" rx="1.5" fill="#7dd3fc" />
    </svg>
  );
}

export function ConsoleShell({ title, subtitle, runs, selectedRunId, onSelectRun, children }: Props) {
  const healthQ = useHealth();
  const summaryQ = useRunSummary(selectedRunId);
  const [downloadStatus, setDownloadStatus] = useState<{ kind: 'ok' | 'err'; text: string } | null>(null);

  async function handleDownloadEvidence() {
    if (!selectedRunId) {
      setDownloadStatus({ kind: 'err', text: 'Select a run before exporting evidence.' });
      return;
    }
    try {
      const [sum, rec] = await Promise.all([
        geoApi.getRunSummary(selectedRunId),
        geoApi.getReconciliation(selectedRunId),
      ]);
      const payload = JSON.stringify({ runId: selectedRunId, summary: sum, reconciliation: rec }, null, 2);
      const blob = new Blob([payload], { type: 'application/json' });
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `geostream-${selectedRunId}-evidence.json`;
      a.click();
      URL.revokeObjectURL(url);
      setDownloadStatus({ kind: 'ok', text: `Exported geostream-${selectedRunId}-evidence.json` });
    } catch {
      setDownloadStatus({ kind: 'err', text: 'Could not export run evidence right now.' });
    }
  }

  const mode = summaryQ.data?.mode ?? 'RECORDED';
  const freshness = summaryQ.data?.dataFreshnessAt
    ? new Date(summaryQ.data.dataFreshnessAt).toLocaleTimeString([], { hour12: false })
    : '—';

  const deps = healthQ.data?.dependencies;

  return (
    <div className="gs-shell">
      <header className="gs-topbar-shell">
        <div className="gs-topbar-inner">
          <div className="gs-brand">
            <GeoStreamLogo />
            <div>
              <div style={{ fontWeight: 700, fontSize: '15px', color: 'var(--ink)', display: 'flex', alignItems: 'center', gap: '8px' }}>
                <span>GeoStream</span>
                <span className="gs-beacon" aria-hidden="true" />
              </div>
              <div style={{ fontSize: '11px', color: 'var(--muted)' }}>
                Synthetic Telemetry · PG + Redis + Kafka
              </div>
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
        </div>

        <div className="gs-command-bar">
          <div className="gs-topbar-group">
            <label htmlFor="run-select" style={{ fontSize: '12px', fontWeight: 600, color: 'var(--muted)' }}>
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
            <button type="button" className="gs-btn gs-btn-primary" onClick={() => void handleDownloadEvidence()}>
              Download evidence
            </button>
          </div>
        </div>
      </header>

      <main className="gs-main">
        {downloadStatus ? (
          <div
            role="status"
            className="gs-panel"
            style={{
              padding: '10px 14px',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'space-between',
              gap: '10px',
              borderColor: downloadStatus.kind === 'ok' ? 'var(--accent-glow)' : 'var(--danger)',
              color: downloadStatus.kind === 'ok' ? 'var(--accent-text)' : 'var(--danger)',
              fontSize: '13px',
            }}
          >
            <span>{downloadStatus.text}</span>
            <button type="button" className="gs-btn" style={{ minHeight: '30px', padding: '0 10px' }} onClick={() => setDownloadStatus(null)}>
              Dismiss
            </button>
          </div>
        ) : null}

        <div className="gs-page-hero">
          <div>
            <h1 className="gs-page-title">{title}</h1>
            <p className="gs-page-sub">{subtitle}</p>
          </div>
          <div style={{ fontSize: '12px', color: 'var(--muted)' }}>
            PG owns durable transitions · Redis caches latest state (24h TTL)
          </div>
        </div>
        {children}
      </main>
    </div>
  );
}
