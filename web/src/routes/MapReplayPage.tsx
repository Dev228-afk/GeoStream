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
  const [inspectedFenceId, setInspectedFenceId] = useState<string | null>(null);
  const [inspectedDeviceId, setInspectedDeviceId] = useState<string | null>(null);

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
  const focusedDevice =
    devices.find((d) => d.DeviceID === inspectedDeviceId) ??
    devices.find((d) => d.DeviceID === activeTransition?.deviceId) ??
    null;

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '20px' }}>
      <div className="gs-map-grid">
        <section className="gs-panel" aria-label="Synthetic city geofence canvas">
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '12px', flexWrap: 'wrap', gap: '8px' }}>
            <div>
              <h2 style={{ margin: 0, fontSize: '18px', color: 'var(--ink)' }}>
                Geofence polygons &amp; sampled couriers
              </h2>
              <div className="gs-page-sub">
                Click any polygon or courier pin · Max 100 visible pins ({devices.length} shown)
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
            style={{ width: '100%', height: 'auto', background: '#090d16', borderRadius: 'var(--r-sm)', border: '1px solid var(--line-strong)' }}
          >
            {/* Coordinate radar gridlines (uses rect/line only, never circle) */}
            {[90, 210, 330].map((y) => (
              <line key={`h-${y}`} x1="20" y1={y} x2="700" y2={y} stroke="#1e293b" strokeDasharray="3 5" strokeWidth="1" />
            ))}
            {[140, 310, 480, 620].map((x) => (
              <line key={`v-${x}`} x1={x} y1="20" x2={x} y2="395" stroke="#1e293b" strokeDasharray="3 5" strokeWidth="1" />
            ))}

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
                const isHighlight = activeTransition?.fenceId === f.id || inspectedFenceId === f.id;
                return (
                  <polygon
                    key={`${f.id}-${rIdx}`}
                    points={pts}
                    fill={
                      isHole
                        ? '#090d16'
                        : isHighlight
                          ? 'rgba(56, 189, 248, 0.28)'
                          : 'rgba(56, 189, 248, 0.11)'
                    }
                    stroke={isHole ? '#fb7185' : isHighlight ? '#7dd3fc' : '#38bdf8'}
                    strokeWidth={isHighlight ? 2.4 : 1.3}
                    strokeDasharray={isHole ? '4 2' : undefined}
                    style={{ cursor: isHole ? 'default' : 'pointer' }}
                    onClick={() => {
                      if (!isHole) setInspectedFenceId((cur) => (cur === f.id ? null : f.id));
                    }}
                  />
                );
              });
            })}

            {devices.map((d) => {
              const [cx, cy] = project(d.Lon, d.Lat);
              const isInside = (d.InsideFenceIDs?.length ?? 0) > 0;
              const isSelected = inspectedDeviceId === d.DeviceID || activeTransition?.deviceId === d.DeviceID;
              return (
                <circle
                  key={d.DeviceID}
                  cx={cx}
                  cy={cy}
                  r={isSelected ? 6.5 : 3.8}
                  fill={isSelected ? '#f8fafc' : isInside ? '#34d399' : '#38bdf8'}
                  stroke={isSelected ? '#0284c7' : undefined}
                  strokeWidth={isSelected ? 2 : undefined}
                  opacity={d.PositionFreshness === 'stale' ? 0.45 : 0.92}
                  style={{ cursor: 'pointer' }}
                  onClick={() => setInspectedDeviceId(d.DeviceID)}
                >
                  <title>{`${d.DeviceID} seq=${d.LastSeq}`}</title>
                </circle>
              );
            })}

            {/* Non-circle legend markers to keep svg circle count strictly <= 100 */}
            <g transform="translate(24, 398)">
              <rect x="0" y="2" width="10" height="10" rx="2" fill="#34d399" />
              <text x="16" y="11" fontSize="11" fill="#cbd5e1">Inside fence</text>
              <rect x="98" y="2" width="10" height="10" rx="2" fill="#38bdf8" />
              <text x="114" y="11" fontSize="11" fill="#cbd5e1">Roaming courier</text>
              <rect x="218" y="2" width="10" height="10" rx="2" fill="none" stroke="#fb7185" strokeWidth="1.5" />
              <text x="234" y="11" fontSize="11" fill="#cbd5e1">Exclusion hole</text>
              <text x="480" y="11" fontSize="11" fill="#94a3b8" className="gs-num">
                {MIN_LAT.toFixed(2)}..{MAX_LAT.toFixed(2)}N · {MIN_LON.toFixed(2)}..{MAX_LON.toFixed(2)}E
              </text>
            </g>
          </svg>

          {(inspectedFenceId || focusedDevice) ? (
            <div
              role="status"
              style={{
                marginTop: '12px',
                padding: '10px 14px',
                borderRadius: 'var(--r-sm)',
                background: 'var(--surface-muted)',
                border: '1px solid var(--line-strong)',
                display: 'flex',
                flexWrap: 'wrap',
                alignItems: 'center',
                justifyContent: 'space-between',
                gap: '8px',
                fontSize: '13px',
              }}
            >
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: '12px' }}>
                {inspectedFenceId ? (
                  <span>
                    Selected fence: <code>{inspectedFenceId}</code>
                  </span>
                ) : null}
                {focusedDevice ? (
                  <span className="gs-num">
                    Courier: <code>{focusedDevice.DeviceID}</code> · seq {focusedDevice.LastSeq} ({focusedDevice.Lat.toFixed(4)}, {focusedDevice.Lon.toFixed(4)})
                  </span>
                ) : null}
              </div>
              <button
                type="button"
                className="gs-btn"
                style={{ minHeight: '32px', padding: '0 10px' }}
                onClick={() => {
                  setInspectedFenceId(null);
                  setInspectedDeviceId(null);
                }}
              >
                Clear focus
              </button>
            </div>
          ) : null}
        </section>

        <aside className="gs-panel" aria-label="Replay scrubber and transition feed">
          <h2 style={{ margin: '0 0 12px', fontSize: '18px', color: 'var(--ink)' }}>Event replay scrubber</h2>
          <div style={{ display: 'flex', alignItems: 'center', flexWrap: 'wrap', gap: '8px', marginBottom: '12px' }}>
            <button
              type="button"
              className="gs-btn gs-btn-primary"
              onClick={() => setPlaying((p) => !p)}
              disabled={transitions.length === 0}
            >
              {playing ? 'Pause replay' : 'Play replay'}
            </button>
            <button
              type="button"
              className="gs-btn"
              onClick={() => {
                setPlaying(false);
                setStepIdx((s) => Math.max(0, s - 1));
              }}
              disabled={stepIdx <= 0}
            >
              Prev
            </button>
            <button
              type="button"
              className="gs-btn"
              onClick={() => {
                setPlaying(false);
                setStepIdx((s) => Math.min(maxStep, s + 1));
              }}
              disabled={stepIdx >= maxStep}
            >
              Next
            </button>
          </div>
          <div className="gs-num" style={{ fontSize: '13px', color: 'var(--muted)', marginBottom: '8px' }}>
            Step {transitions.length ? stepIdx + 1 : 0} / {transitions.length}
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
            style={{ width: '100%', minHeight: '44px', accentColor: '#38bdf8' }}
          />

          {activeTransition ? (
            <div style={{ marginTop: '14px', padding: '12px', background: 'var(--surface-muted)', border: '1px solid var(--line-strong)', borderRadius: 'var(--r-sm)' }}>
              <div style={{ fontWeight: 650 }}>
                <span className={`gs-badge ${activeTransition.type === 'ENTER' ? 'gs-badge-success' : 'gs-badge-warn'}`}>
                  {activeTransition.type}
                </span>{' '}
                <code>{activeTransition.fenceId}</code>
              </div>
              <div style={{ fontSize: '13px', marginTop: '8px' }} className="gs-num">
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

      <section className="gs-panel" aria-label="Accessible courier sample table">
        <h2 style={{ margin: '0 0 12px', fontSize: '18px', color: 'var(--ink)' }}>
          Sampled courier positions (accessible table alternative)
        </h2>
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
                {devices.slice(0, 15).map((d) => {
                  const rowFocused = focusedDevice?.DeviceID === d.DeviceID;
                  return (
                    <tr
                      key={d.DeviceID}
                      onClick={() => setInspectedDeviceId(d.DeviceID)}
                      style={{
                        cursor: 'pointer',
                        background: rowFocused ? 'rgba(56, 189, 248, 0.08)' : undefined,
                      }}
                    >
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
