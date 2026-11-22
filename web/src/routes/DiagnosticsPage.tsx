import { useBackpressureEvidence, useReconciliation } from '../api/hooks';

export function DiagnosticsPage({ runId }: { runId: string }) {
  const bpQ = useBackpressureEvidence();
  const recQ = useReconciliation(runId);

  const bp = bpQ.data;
  const maxRate = bp ? Math.max(bp.baseline.throughputEventsPerSec, bp.bounded.throughputEventsPerSec, 1) : 1;
  const offsets = (recQ.data?.lastCommittedOffsets ?? []).slice(0, 16);
  const maxOffset = offsets.reduce((m, p) => (p.endOffset > m ? p.endOffset : m), 1);

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '20px' }}>
      <section className="gs-panel" aria-label="Controlled backpressure regression experiment">
        <div style={{ display: 'flex', justifyContent: 'space-between', flexWrap: 'wrap', gap: '8px', marginBottom: '12px' }}>
          <div>
            <h2 style={{ margin: 0, fontSize: '18px', color: 'var(--ink)' }}>
              Bounded channel vs. unbounded baseline experiment
            </h2>
            <div className="gs-page-sub">
              Isolated test comparing identical seeded streams &amp; DB contention without silent event drops
            </div>
          </div>
          {bp ? (
            <span className={`gs-badge ${bp.identicalOutputCounts ? 'gs-badge-success' : 'gs-badge-danger'}`}>
              Output integrity: {bp.identicalOutputCounts ? `Identical (${bp.persistedCount} == ${bp.persistedCount})` : 'MISMATCH'}
            </span>
          ) : null}
        </div>

        {!bp ? (
          <div style={{ color: 'var(--muted)' }}>Loading backpressure benchmark evidence...</div>
        ) : (
          <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
            <div
              style={{
                padding: '12px 14px',
                borderRadius: 'var(--r-sm)',
                background: 'var(--surface-muted)',
                border: '1px solid var(--line-strong)',
                fontSize: '13px',
                color: 'var(--subtle)',
              }}
            >
              <strong style={{ color: 'var(--ink)' }}>Measured delta:</strong> Unbounded faulty mode ran at{' '}
              <strong className="gs-num" style={{ color: 'var(--danger)' }}>
                {bp.measuredDeltaVsFixedPct.toFixed(1)}%
              </strong>{' '}
              lower throughput than bounded worker pool ({bp.classification}; compared honestly against{' '}
              <em>{bp.historicalResumeClaim}</em>).
            </div>

            <svg
              viewBox="0 0 680 112"
              role="img"
              aria-label="Throughput comparison bar chart"
              style={{ width: '100%', height: '112px', background: '#090d16', borderRadius: 'var(--r-sm)', border: '1px solid var(--line)' }}
            >
              <text x="14" y="36" fontSize="13" fill="#cbd5e1">Unbounded faulty baseline</text>
              <rect x="216" y="20" width={Math.max(4, (bp.baseline.throughputEventsPerSec / maxRate) * 336)} height="24" rx="5" fill="#fb7185" />
              <text x="566" y="36" fontSize="13" fill="#f8fafc" className="gs-num">
                {bp.baseline.throughputEventsPerSec.toFixed(1)} ev/s
              </text>

              <text x="14" y="80" fontSize="13" fill="#cbd5e1">Bounded channel + pool</text>
              <rect x="216" y="64" width={Math.max(4, (bp.bounded.throughputEventsPerSec / maxRate) * 336)} height="24" rx="5" fill="#38bdf8" />
              <text x="566" y="80" fontSize="13" fill="#f8fafc" className="gs-num">
                {bp.bounded.throughputEventsPerSec.toFixed(1)} ev/s
              </text>
            </svg>

            <div className="gs-table-wrap" tabIndex={0} role="region" aria-label="Backpressure comparison table">
              <table className="gs-table">
                <thead>
                  <tr>
                    <th>Metric</th>
                    <th>Unbounded Baseline (Controlled Faulty)</th>
                    <th>Bounded After Fix (Production Pattern)</th>
                  </tr>
                </thead>
                <tbody>
                  <tr>
                    <td>Persisted Transitions (zero drops)</td>
                    <td className="gs-num">{bp.baseline.persistedCount}</td>
                    <td className="gs-num">{bp.bounded.persistedCount}</td>
                  </tr>
                  <tr>
                    <td>Throughput (events/sec)</td>
                    <td className="gs-num">{bp.baseline.throughputEventsPerSec.toFixed(1)}</td>
                    <td className="gs-num">{bp.bounded.throughputEventsPerSec.toFixed(1)}</td>
                  </tr>
                  <tr>
                    <td>Max Queue Depth</td>
                    <td className="gs-num">{bp.baseline.maxQueueDepth}</td>
                    <td className="gs-num">{bp.bounded.maxQueueDepth}</td>
                  </tr>
                  <tr>
                    <td>Peak Heap (MB)</td>
                    <td className="gs-num">{bp.baseline.peakHeapMb.toFixed(2)} MB</td>
                    <td className="gs-num">{bp.bounded.peakHeapMb.toFixed(2)} MB</td>
                  </tr>
                  <tr>
                    <td>p99 Persistence Latency (ms)</td>
                    <td className="gs-num">{bp.baseline.p99Ms.toFixed(2)} ms</td>
                    <td className="gs-num">{bp.bounded.p99Ms.toFixed(2)} ms</td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>
        )}
      </section>

      <section className="gs-panel" aria-label="Kafka partition offsets">
        <h2 style={{ margin: '0 0 12px', fontSize: '18px', color: 'var(--ink)' }}>
          Kafka partition offsets ({runId})
        </h2>
        {!recQ.data ? (
          <div style={{ color: 'var(--muted)' }}>Loading partition offsets...</div>
        ) : (
          <div className="gs-table-wrap" tabIndex={0} role="region" aria-label="Kafka partition offsets table">
            <table className="gs-table">
              <thead>
                <tr>
                  <th>Partition</th>
                  <th>Start Offset</th>
                  <th>End Offset</th>
                  <th>Committed Offset</th>
                  <th>Progress</th>
                </tr>
              </thead>
              <tbody>
                {offsets.map((p) => {
                  const pct = Math.min(100, Math.round((p.committedOffset / maxOffset) * 100));
                  return (
                    <tr key={p.partition}>
                      <td className="gs-num">P{p.partition}</td>
                      <td className="gs-num">{p.startOffset}</td>
                      <td className="gs-num">{p.endOffset}</td>
                      <td className="gs-num">{p.committedOffset}</td>
                      <td style={{ minWidth: '110px' }}>
                        <div style={{ height: '7px', borderRadius: '999px', background: 'var(--surface-muted)', overflow: 'hidden' }}>
                          <div className="gs-bar-fill" style={{ width: `${Math.max(6, pct)}%`, height: '100%', background: '#38bdf8' }} />
                        </div>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </section>
    </div>
  );
}
