import { useState } from 'react';
import { type TransitionRowDto } from '../api/client';
import { useTransitions } from '../api/hooks';

export function TransitionsPage({ runId }: { runId: string }) {
  const [typeFilter, setTypeFilter] = useState<string>('');
  const [cursor, setCursor] = useState<string>('');
  const [selected, setSelected] = useState<TransitionRowDto | null>(null);
  const [copiedId, setCopiedId] = useState(false);

  const q = useTransitions(runId, typeFilter, cursor);
  const items = q.data?.items ?? [];

  async function handleCopyEventId(eventId: string) {
    try {
      await navigator.clipboard?.writeText(eventId);
      setCopiedId(true);
      window.setTimeout(() => setCopiedId(false), 1800);
    } catch {
      setCopiedId(false);
    }
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '20px' }}>
      <div className="gs-panel" style={{ display: 'flex', gap: '10px', flexWrap: 'wrap', alignItems: 'center' }}>
        <span style={{ fontSize: '13px', fontWeight: 600, color: 'var(--muted)' }}>Filter type:</span>
        {['', 'ENTER', 'EXIT'].map((opt) => (
          <button
            key={opt || 'ALL'}
            type="button"
            className={`gs-btn ${typeFilter === opt ? 'gs-btn-primary' : ''}`}
            onClick={() => {
              setCursor('');
              setTypeFilter(opt);
            }}
          >
            {opt || 'All'}
          </button>
        ))}
        {q.data?.nextCursor ? (
          <button
            type="button"
            className="gs-btn"
            style={{ marginLeft: 'auto' }}
            onClick={() => setCursor(q.data?.nextCursor ?? '')}
          >
            Next page →
          </button>
        ) : null}
        {cursor ? (
          <button type="button" className="gs-btn" onClick={() => setCursor('')}>
            First page
          </button>
        ) : null}
      </div>

      <section className="gs-panel" aria-label="Durable geofence transitions">
        {items.length === 0 ? (
          <div style={{ color: 'var(--muted)' }}>No transitions matching filter.</div>
        ) : (
          <>
            <div className="gs-desktop-table gs-table-wrap" tabIndex={0} role="region" aria-label="Durable transitions table">
              <table className="gs-table">
                <thead>
                  <tr>
                    <th>Transition ID</th>
                    <th>Type</th>
                    <th>Device</th>
                    <th>Fence</th>
                    <th>Seq</th>
                    <th>Event Time</th>
                    <th>Details</th>
                  </tr>
                </thead>
                <tbody>
                  {items.map((tr) => (
                    <tr key={tr.transitionId}>
                      <td><code>{tr.transitionId}</code></td>
                      <td>
                        <span className={`gs-badge ${tr.type === 'ENTER' ? 'gs-badge-success' : 'gs-badge-warn'}`}>
                          {tr.type}
                        </span>
                      </td>
                      <td><code>{tr.deviceId}</code></td>
                      <td><code>{tr.fenceId} v{tr.fenceVersion}</code></td>
                      <td className="gs-num">{tr.sourceSequence}</td>
                      <td className="gs-num">{tr.eventTime}</td>
                      <td>
                        <button
                          type="button"
                          className="gs-btn"
                          onClick={() => {
                            setCopiedId(false);
                            setSelected(tr);
                          }}
                        >
                          Inspect
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>

            <div className="gs-mobile-cards">
              {items.map((tr) => (
                <div key={tr.transitionId} className="gs-panel" style={{ padding: '14px', background: 'var(--surface-elevated)' }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: '8px' }}>
                    <span className={`gs-badge ${tr.type === 'ENTER' ? 'gs-badge-success' : 'gs-badge-warn'}`}>{tr.type}</span>
                    <code style={{ fontSize: '12px' }}>{tr.fenceId}</code>
                  </div>
                  <div style={{ marginTop: '8px', fontSize: '13px', color: 'var(--subtle)' }}>
                    <code>{tr.deviceId}</code> · seq <span className="gs-num">{tr.sourceSequence}</span>
                  </div>
                  <button
                    type="button"
                    className="gs-btn"
                    style={{ marginTop: '10px', width: '100%' }}
                    onClick={() => {
                      setCopiedId(false);
                      setSelected(tr);
                    }}
                  >
                    Inspect source provenance
                  </button>
                </div>
              ))}
            </div>
          </>
        )}
      </section>

      {selected ? (
        <aside className="gs-panel" aria-label="Selected transition detail drawer" style={{ borderColor: 'var(--accent-glow)' }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: '10px' }}>
            <h2 style={{ margin: 0, fontSize: '18px', color: 'var(--ink)' }}>
              Transition detail: <code>{selected.transitionId}</code>
            </h2>
            <div style={{ display: 'flex', gap: '8px', flexWrap: 'wrap' }}>
              <button
                type="button"
                className="gs-btn"
                onClick={() => void handleCopyEventId(selected.sourceEventId)}
              >
                {copiedId ? 'Copied' : 'Copy Event ID'}
              </button>
              <button type="button" className="gs-btn" onClick={() => setSelected(null)}>
                Close
              </button>
            </div>
          </div>
          <dl style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: '12px', marginTop: '14px', fontSize: '13px' }}>
            <div><dt style={{ color: 'var(--muted)' }}>Source Event ID</dt><dd style={{ margin: '4px 0 0' }}><code>{selected.sourceEventId}</code></dd></div>
            <div><dt style={{ color: 'var(--muted)' }}>Partition / Offset</dt><dd style={{ margin: '4px 0 0', color: 'var(--ink)' }} className="gs-num">P{selected.sourcePartition} @ {selected.sourceOffset}</dd></div>
            <div><dt style={{ color: 'var(--muted)' }}>Event Time (UTC)</dt><dd style={{ margin: '4px 0 0', color: 'var(--ink)' }} className="gs-num">{selected.eventTime}</dd></div>
            <div><dt style={{ color: 'var(--muted)' }}>PG Persisted At</dt><dd style={{ margin: '4px 0 0', color: 'var(--ink)' }} className="gs-num">{selected.createdAt}</dd></div>
          </dl>
        </aside>
      ) : null}
    </div>
  );
}
