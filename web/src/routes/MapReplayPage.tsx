import { useEffect, useState } from 'react';
import { useDevices, useGeofences, useTransitions } from '../api/hooks';

const MIN_LON = -121.96;
const MAX_LON = -121.81;
const MIN_LAT = 37.27;
const MAX_LAT = 37.39;

function project(lon: number, lat: number): [number, number] {
  const x = ((lon - MIN_LON) / (MAX_LON - MIN_LON)) * 680 + 20;
  const y = 390 - ((lat - MIN_LAT) / (MAX_LAT - MIN_LAT)) * 360;
  return [x, y];
}

export function MapReplayPage({ runId }: { runId: string }) {
  const geoQ = useGeofences(runId);
  const devQ = useDevices(runId);
  const trQ = useTransitions(runId, '', '');

  // Bounded <= 100 sampled devices for zero DOM blowout
  const devices = (devQ.data?.items ?? []).slice(0, 100);
  const transitions = trQ.data?.items ?? [];
  const fences = geoQ.data?.features ?? [];

  const [stepIdx, setStepIdx] = useState(0);
  const [playing, setPlaying] = useState(false);

  const maxStep = Math.max(transitions.length - 1, 0);

  // Throttled playback <= 5fps (200ms tick)
  useEffect(() => {
    if (!playing || maxStep === 0) return;
    const timer = window.setInterval(() => {
      setStepIdx((prev) => (prev >= maxStep ? 0 : prev + 1));
    }, 200);
    return () => window.clearInterval(timer);
  }, [playing, maxStep]);

  const activeTransition = transitions[stepIdx];
  const staleCount = devices.filter((d) => d.PositionFreshness === 'stale').length;

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '20px' }}>
      <div className="gs-map-grid">
        <section className="gs-panel" aria-label="Synthetic city geofence canvas">
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '12px', flexWrap: 'wrap', gap: '8px' }}>
            <div>
              <h2 style={{ margin: 0, fontSize: '18px' }}>Geofence polygons &amp; sampled couriers</h2>
              <div className="gs-page-sub">
                Synthetic grid (boundary=inside, holes=outside) · Max 100 visible pins ({devices.length} shown)
              </div>
            </div>
            {staleCount > 0 ? (
              <span className="gs-badge gs-badge-warn">Position freshness: stale ({staleCount})</span>
            ) : (
              <span className="gs-badge gs-badge-success">Position freshness: live</span>
            )}
          </div>

          <svg
            viewBox="0 0 720 420"
            role="img"
            aria-label="GeoJSON geofence polygons and sampled simulated courier positions"
            style={{ width: '100%', height: 'auto', background: '#f8fafc', borderRadius: 'var(--r-sm)', border: '1px solid var(--line)' }}
          >
            {fences.map((f) => {
              const rawGeom = f.geometry as unknown;
              const rings: number[][][] = Array.isArray(rawGeom)
                ? (rawGeom as number[][][])
                : f.geometry?.type === 'Polygon'
                  ? ((f.geometry.coordinates as number[][][]) ?? [])
                  : ((f.geometry?.coordinates as number[][][][])?.[0] ?? []);
              return rings.map((ring, rIdx) => {
                const pts = ring.map((c) => project(c[0], c[1]).join(',')).join(' ');
                const isHole = rIdx > 0;
                const isHighlight = activeTransition?.fenceId === f.id;
                return (
                  <polygon
                    key={`${f.id}-${rIdx}`}
                    points={pts}
                    fill={isHole ? '#ffffff' : isHighlight ? 'rgba(23, 106, 146, 0.28)' : 'rgba(23, 106, 146, 0.10)'}
                    stroke={isHole ? '#9e2d31' : '#176a92'}
                    strokeWidth={isHighlight ? 2.4 : 1.3}
                    strokeDasharray={isHole ? '4 2' : undefined}
                  />
                );
              });
            })}

            {devices.map((d) => {
              const [cx, cy] = project(d.Lon, d.Lat);
              const isInside = (d.InsideFenceIDs?.length ?? 0) > 0;
              return (
                <circle
                  key={d.DeviceID}
                  cx={cx}
                  cy={cy}
                  r={activeTransition?.deviceId === d.DeviceID ? 6.5 : 3.5}
                  fill={isInside ? '#105e47' : '#17222b'}
                  opacity={d.PositionFreshness === 'stale' ? 0.45 : 0.88}
                >
                  <title>{`${d.DeviceID} seq=${d.LastSeq}`}</title>
                </circle>
              );
            })}
          </svg>
        </section>

        <aside className="gs-panel" aria-label="Replay scrubber and transition feed">
          <h2 style={{ margin: '0 0 12px', fontSize: '18px' }}>Event replay scrubber</h2>
          <div style={{ display: 'flex', alignItems: 'center', gap: '10px', marginBottom: '12px' }}>
            <button
              type="button"
              className="gs-btn gs-btn-primary"
              onClick={() => setPlaying((p) => !p)}
              disabled={transitions.length === 0}
            >
              {playing ? 'Pause replay' : 'Play replay'}
            </button>
            <span className="gs-num" style={{ fontSize: '13px', color: 'var(--muted)' }}>
              Step {transitions.length ? stepIdx + 1 : 0} / {transitions.length}
            </span>
          </div>

          <label htmlFor="replay-range" style={{ display: 'block', fontSize: '13px', fontWeight: 600, marginBottom: '6px' }}>
            Timeline step
          </label>
          <input
            id="replay-range"
            type="range"
            min={0}
            max={maxStep}
            value={stepIdx}
            onChange={(e) => {
              setPlaying(false);
              setStepIdx(Number(e.target.value));
            }}
            style={{ width: '100%', minHeight: '44px' }}
          />

          {activeTransition ? (
            <div style={{ marginTop: '14px', padding: '12px', background: 'var(--surface-muted)', borderRadius: 'var(--r-sm)' }}>
              <div style={{ fontWeight: 650 }}>
                <span className={`gs-badge ${activeTransition.type === 'ENTER' ? 'gs-badge-success' : 'gs-badge-warn'}`}>
                  {activeTransition.type}
                </span>{' '}
                <code>{activeTransition.fenceId}</code>
              </div>
              <div style={{ fontSize: '13px', marginTop: '6px' }} className="gs-num">
                Device: <code>{activeTransition.deviceId}</code> · Seq: {activeTransition.sourceSequence}
              </div>
              <div style={{ fontSize: '12px', color: 'var(--muted)', marginTop: '4px' }} className="gs-num">
                {activeTransition.eventTime}
              </div>
            </div>
          ) : (
            <div style={{ marginTop: '12px', fontSize: '13px', color: 'var(--muted)' }}>
              No transitions recorded for replay.
            </div>
          )}
        </aside>
      </div>

      {/* Accessible tabular alternative per WCAG / Section G */}
      <section className="gs-panel" aria-label="Accessible courier sample table">
        <h2 style={{ margin: '0 0 12px', fontSize: '18px' }}>Sampled courier positions (accessible table alternative)</h2>
        {devices.length === 0 ? (
          <div style={{ fontSize: '14px', color: 'var(--muted)' }}>
            No cached device state currently active in Redis for <code>{runId}</code>.
          </div>
        ) : (
          <div className="gs-table-wrap" tabIndex={0} role="region" aria-label="Sampled courier positions table">
            <table className="gs-table">
              <thead>
                <tr>
                  <th>Courier ID</th>
                  <th>Last Seq</th>
                  <th>Coordinates</th>
                  <th>Inside fences</th>
                  <th>Freshness</th>
                </tr>
              </thead>
              <tbody>
                {devices.slice(0, 15).map((d) => (
                  <tr key={d.DeviceID}>
                    <td><code>{d.DeviceID}</code></td>
                    <td className="gs-num">{d.LastSeq}</td>
                    <td className="gs-num">{d.Lat.toFixed(4)}, {d.Lon.toFixed(4)}</td>
                    <td>{d.InsideFenceIDs?.length ? d.InsideFenceIDs.join(', ') : 'Outside all'}</td>
                    <td>
                      <span className={`gs-badge ${d.PositionFreshness === 'live' ? 'gs-badge-success' : 'gs-badge-warn'}`}>
                        {d.PositionFreshness}
                      </span>
                    </td>
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

