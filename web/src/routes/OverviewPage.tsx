import { useRunSummary, useTimeseries, useIncidents } from '../api/hooks';

export function OverviewPage({ runId }: { runId: string }) {
  const sumQ = useRunSummary(runId);
  const tsQ = useTimeseries(runId, 'events_accepted_per_sec');
  const incQ = useIncidents(runId);

  const s = sumQ.data;
  const pts = tsQ.data?.points ?? [];
  const validPts = pts.filter((p): p is { at: string; value: number } => p.value !== null);
  const maxVal = validPts.reduce((m, p) => (p.value > m ? p.value : m), 1);

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '20px' }}>
      {sumQ.error ? (
        <div className="gs-panel" role="alert" style={{ borderColor: 'var(--danger)' }}>
          <strong style={{ color: 'var(--danger)' }}>Unable to load run summary</strong>
          <div className="gs-page-sub">{(sumQ.error as Error).message}</div>
        </div>
      ) : null}

      {/* Exact 4 compact metrics band per Section G */}
      <section className="gs-band-4" aria-label="Primary telemetry indicators">
        <div className="gs-panel">
          <div className="gs-metric-label">Consumed rate</div>
          <div className="gs-metric-val gs-num">
            {s ? `${s.consumedPerSec.toFixed(1)} /s` : '—'}
          </div>
          <div className="gs-metric-meta gs-num">
            Generated: {s ? `${s.generatedPerSec.toFixed(1)}/s` : '—'}
          </div>
        </div>

        <div className="gs-panel">
          <div className="gs-metric-label">Latency p99 ({s?.latency.boundary ?? 'INGEST_INTERNAL'})</div>
          <div className="gs-metric-val gs-num">
            {s ? `${s.latency.p99Ms.toFixed(1)} ms` : '—'}
          </div>
          <div className="gs-metric-meta gs-num">
            p50: {s ? `${s.latency.p50Ms.toFixed(1)} ms` : '—'} · Target: &lt;12 ms (cluster)
          </div>
        </div>

        <div className="gs-panel">
          <div className="gs-metric-label">Kafka consumer lag</div>
          <div className="gs-metric-val gs-num">{s ? s.kafkaLag : '—'}</div>
          <div className="gs-metric-meta">
            Broker acked: {s ? `${s.brokerAckedPerSec.toFixed(1)}/s` : '—'}
          </div>
        </div>

        <div className="gs-panel">
          <div className="gs-metric-label">Active Redis couriers</div>
          <div className="gs-metric-val gs-num">{s ? s.activeDeviceCount.toLocaleString() : '—'}</div>
          <div className="gs-metric-meta">
            Mode: {s?.mode ?? '—'} ({s?.metricsStatus ?? 'loading'})
          </div>
        </div>
      </section>

      {/* Dominant throughput composite chart or honest unavailable notice */}
      <section className="gs-panel" aria-label="Event throughput timeseries">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: '8px', marginBottom: '14px' }}>
          <div>
            <h2 style={{ margin: 0, fontSize: '18px' }}>Accepted throughput stream</h2>
            <div className="gs-page-sub">
              Source: Prometheus ({tsQ.data?.metric ?? 'events_accepted_per_sec'}, unit: {tsQ.data?.unit ?? 'events/s'}, 5m window)
            </div>
          </div>
          <span className={`gs-badge ${tsQ.data?.status === 'ok' ? 'gs-badge-success' : 'gs-badge-warn'}`}>
            Prometheus: {tsQ.data?.status ?? 'checking'}
          </span>
        </div>

        {tsQ.data?.status === 'unavailable' || validPts.length === 0 ? (
          <div
            role="status"
            style={{
              padding: '20px',
              background: 'var(--surface-muted)',
              borderRadius: 'var(--r-sm)',
              border: '1px dashed var(--line)',
            }}
          >
            <strong>Metrics temporarily unavailable (no fabricated fallback points)</strong>
            <div style={{ fontSize: '13px', color: 'var(--muted)', marginTop: '4px' }}>
              Last observed run data timestamp: {s?.dataFreshnessAt ?? 'none'}. Historical summary metrics reflect committed DB/evidence totals rather than interpolated lines.
            </div>
          </div>
        ) : (
          <div>
            <svg
              viewBox="0 0 800 220"
              role="img"
              aria-label="Accepted events per second timeseries chart"
              style={{ width: '100%', height: '220px', display: 'block' }}
            >
              <line x1="48" y1="20" x2="48" y2="185" stroke="#d9e2e9" strokeWidth="1" />
              <line x1="48" y1="185" x2="780" y2="185" stroke="#d9e2e9" strokeWidth="1" />
              <text x="8" y="30" fontSize="11" fill="#52616f">{maxVal.toFixed(0)}/s</text>
              <text x="16" y="185" fontSize="11" fill="#52616f">0/s</text>
              <polyline
                fill="none"
                stroke="#176a92"
                strokeWidth="2.5"
                points={pts
                  .map((p, idx) => {
                    if (p.value === null) return '';
                    const x = 48 + (idx / Math.max(pts.length - 1, 1)) * 732;
                    const y = 185 - (p.value / maxVal) * 155;
                    return `${x.toFixed(1)},${y.toFixed(1)}`;
                  })
                  .filter(Boolean)
                  .join(' ')}
              />
            </svg>
            <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '12px', color: 'var(--muted)', marginTop: '6px' }}>
              <span>{pts[0]?.at ?? ''}</span>
              <span>Legend: Accepted unique events/sec (10s resolution)</span>
              <span>{pts[pts.length - 1]?.at ?? ''}</span>
            </div>
          </div>
        )}
      </section>

      {/* Operational incident panel */}
      <section className="gs-panel" aria-label="Operational incidents and alerts">
        <h2 style={{ margin: '0 0 12px', fontSize: '18px' }}>Operational incidents</h2>
        {!incQ.data || incQ.data.items.length === 0 ? (
          <div style={{ fontSize: '14px', color: 'var(--muted)' }}>
            No persisted incidents recorded for run <code>{runId}</code>.
          </div>
        ) : (
          <div className="gs-table-wrap" tabIndex={0} role="region" aria-label="Operational incidents table">
            <table className="gs-table">
              <thead>
                <tr>
                  <th>Kind</th>
                  <th>Severity</th>
                  <th>First seen</th>
                  <th>Last seen</th>
                  <th>Evidence reference</th>
                </tr>
              </thead>
              <tbody>
                {incQ.data.items.map((inc) => (
                  <tr key={inc.id}>
                    <td><code>{inc.kind}</code></td>
                    <td>
                      <span className={`gs-badge ${inc.severity === 'critical' ? 'gs-badge-danger' : 'gs-badge-warn'}`}>
                        {inc.severity}
                      </span>
                    </td>
                    <td className="gs-num">{new Date(inc.firstAt).toISOString()}</td>
                    <td className="gs-num">{new Date(inc.lastAt).toISOString()}</td>
                    <td><code>{inc.evidenceRef || '—'}</code></td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>
    </div>
  );
}

