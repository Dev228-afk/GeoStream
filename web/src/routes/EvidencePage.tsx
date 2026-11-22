import { useState } from 'react';
import { useAlertsEvidence, useChaosEvidence, useReconciliation } from '../api/hooks';

export function EvidencePage({ runId }: { runId: string }) {
  const recQ = useReconciliation(runId);
  const chaosQ = useChaosEvidence();
  const alertQ = useAlertsEvidence();
  const [copiedJson, setCopiedJson] = useState(false);

  const rec = recQ.data;
  const chaos = chaosQ.data;

  async function handleCopyJson() {
    if (!alertQ.data) return;
    try {
      await navigator.clipboard?.writeText(JSON.stringify(alertQ.data, null, 2));
      setCopiedJson(true);
      window.setTimeout(() => setCopiedJson(false), 1800);
    } catch {
      setCopiedJson(false);
    }
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '20px' }}>
      <section className="gs-panel" aria-label="Run reconciliation report">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: '8px' }}>
          <h2 style={{ margin: 0, fontSize: '18px', color: 'var(--ink)' }}>
            Produced vs. consumed ledger reconciliation ({runId})
          </h2>
          {rec ? (
            <span className={`gs-badge ${rec.pass ? 'gs-badge-success' : 'gs-badge-danger'}`}>
              {rec.pass ? 'RECONCILIATION PASS' : 'RECONCILIATION FAIL'}
            </span>
          ) : null}
        </div>

        {rec ? (
          <>
            <div className="gs-band-4" style={{ marginTop: '14px' }}>
              <div><div className="gs-metric-label">Generated</div><div className="gs-metric-val gs-num">{rec.generated}</div></div>
              <div><div className="gs-metric-label">Broker acked</div><div className="gs-metric-val gs-num">{rec.brokerAcked}</div></div>
              <div><div className="gs-metric-label">Transitions persisted</div><div className="gs-metric-val gs-num">{rec.transitionsPersisted}</div></div>
              <div><div className="gs-metric-label">Outbox pending / gaps</div><div className="gs-metric-val gs-num">{rec.outboxPending} / {rec.gaps}</div></div>
            </div>
            {rec.failureReasons?.length ? (
              <div style={{ marginTop: '12px', color: 'var(--danger)', fontSize: '13px' }}>
                Reasons: {rec.failureReasons.join('; ')}
              </div>
            ) : null}
            <div style={{ marginTop: '10px', fontSize: '12px', color: 'var(--muted)' }}>
              {(rec.limitations ?? []).join(' ')}
            </div>
          </>
        ) : (
          <div style={{ color: 'var(--muted)', marginTop: '8px' }}>Loading reconciliation...</div>
        )}
      </section>

      <section className="gs-panel" aria-label="Chaos failure matrix">
        <h2 style={{ margin: '0 0 12px', fontSize: '18px', color: 'var(--ink)' }}>
          Chaos &amp; crash-safety verification matrix
        </h2>
        {!chaos ? (
          <div style={{ color: 'var(--muted)' }}>No chaos matrix artifact loaded.</div>
        ) : (
          <div className="gs-table-wrap" tabIndex={0} role="region" aria-label="Chaos verification matrix table">
            <table className="gs-table">
              <thead>
                <tr>
                  <th>Scenario</th>
                  <th>Expected Category</th>
                  <th>Status</th>
                  <th>Detail</th>
                </tr>
              </thead>
              <tbody>
                {chaos.scenarios.map((s) => (
                  <tr key={s.scenario}>
                    <td><code>{s.scenario}</code></td>
                    <td><code>{s.expectedCategory}</code></td>
                    <td><span className="gs-badge gs-badge-success">{s.status}</span></td>
                    <td>{s.detail}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>

      <section className="gs-panel" aria-label="Alert drill provenance">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: '8px', marginBottom: '12px' }}>
          <h2 style={{ margin: 0, fontSize: '18px', color: 'var(--ink)' }}>Prometheus alert drill verification</h2>
          {alertQ.data ? (
            <button type="button" className="gs-btn" onClick={() => void handleCopyJson()}>
              {copiedJson ? 'Copied' : 'Copy JSON'}
            </button>
          ) : null}
        </div>
        {alertQ.data ? (
          <pre
            tabIndex={0}
            role="region"
            aria-label="Prometheus alert drill JSON payload"
            style={{
              margin: 0,
              fontSize: '12px',
              fontFamily: 'var(--font-mono)',
              color: '#cbd5e1',
              overflowX: 'auto',
              background: '#090d16',
              border: '1px solid var(--line-strong)',
              padding: '14px',
              borderRadius: 'var(--r-sm)',
            }}
          >
            {JSON.stringify(alertQ.data, null, 2)}
          </pre>
        ) : (
          <div style={{ fontSize: '13px', color: 'var(--muted)' }}>
            Live Prometheus alert drill artifact not captured yet (<code>evidence/alerts/alert-drill.json</code>).
          </div>
        )}
      </section>
    </div>
  );
}
