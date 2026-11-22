import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { vi, describe, it, expect, beforeEach } from 'vitest';
import { App } from '../App';

const mockRuns = {
  items: [
    {
      id: 'run-local-001',
      seed: 2026,
      profile: 'dev',
      startedAt: '2026-11-22T03:00:00Z',
      endedAt: '2026-11-22T03:00:15Z',
      status: 'COMPLETED',
      generatedCount: 18000,
      brokerAckedCount: 18000,
      configSha256: 'abc',
    },
  ],
  nextCursor: null,
};

const mockSummary = {
  run: mockRuns.items[0],
  generatedPerSec: 1500.2,
  brokerAckedPerSec: 1500.2,
  consumedPerSec: 1498.5,
  acceptedPerSec: 1498.5,
  duplicatePerSec: 0,
  invalidPerSec: 0,
  kafkaLag: 0,
  activeDeviceCount: 1500,
  latency: { boundary: 'INGEST_INTERNAL', p50Ms: 1.5, p95Ms: 4.0, p99Ms: 12.0 },
  dataFreshnessAt: '2026-11-22T03:00:15Z',
  mode: 'RECORDED',
  metricsStatus: 'recorded',
};

describe('GeoStream Monitoring UI', () => {
  beforeEach(() => {
    window.history.pushState({}, '', '/overview');
    vi.stubGlobal(
      'fetch',
      vi.fn(async (input: RequestInfo | URL) => {
        const url = String(input);
        const ok = (body: unknown) =>
          new Response(JSON.stringify(body), { status: 200, headers: { 'Content-Type': 'application/json' } });

        if (url.includes('/api/v1/health')) {
          return ok({ status: 'ok', dependencies: { postgres: 'ok', redis: 'ok', prometheus: 'unavailable' }, commit: 'HEAD' });
        }
        if (url.endsWith('/summary')) return ok(mockSummary);
        if (url.includes('/timeseries')) {
          return ok({ metric: 'events_accepted_per_sec', unit: 'events/s', points: [], source: 'prometheus', status: 'unavailable' });
        }
        if (url.includes('/incidents')) return ok({ items: [], nextCursor: null });
        if (url.includes('/geofences')) {
          return ok({
            type: 'FeatureCollection',
            features: [{ type: 'Feature', id: 'fence-004', properties: { id: 'fence-004', name: 'South', version: 1, enabled: true }, geometry: { type: 'Polygon', coordinates: [[[-121.89, 37.29], [-121.87, 37.29], [-121.87, 37.305], [-121.89, 37.305], [-121.89, 37.29]]] } }],
          });
        }
        if (url.includes('/devices')) {
          return ok({
            items: [{ RunID: 'run-local-001', DeviceID: 'courier-000001', LastSeq: 4, LastEventID: 'e1', Lat: 37.297, Lon: -121.88, EventTime: '2026-11-22T03:00:10Z', ObservedAt: '2026-11-22T03:00:10Z', FenceVersion: 1, InsideFenceIDs: ['fence-004'], PositionFreshness: 'stale' }],
            nextCursor: null,
          });
        }
        if (url.includes('/transitions')) {
          return ok({
            items: [{ transitionId: 'tr_01', runId: 'run-local-001', deviceId: 'courier-000001', fenceId: 'fence-004', fenceVersion: 1, type: 'ENTER', sourceEventId: 'e1', sourceSequence: 2, eventTime: '2026-11-22T03:00:05Z', createdAt: '2026-11-22T03:00:05Z', sourcePartition: 1, sourceOffset: 42 }],
            nextCursor: null,
          });
        }
        if (url.includes('/evidence/backpressure')) {
          return ok({
            runId: 'run-bp-001',
            classification: 'REPRODUCED',
            historicalResumeClaim: '22% throughput drop (HISTORICAL_RESUME_CLAIM)',
            measuredDeltaVsFixedPct: 98.0,
            identicalOutputCounts: true,
            persistedCount: 2400,
            baseline: { mode: 'unbounded_baseline', inputEvents: 2400, expectedCrossings: 2400, persistedCount: 2400, durationMs: 8000, throughputEventsPerSec: 277.0, maxQueueDepth: 2288, peakHeapMb: 9.46, totalGcPauseMs: 0.55, p50Ms: 10, p95Ms: 20, p99Ms: 8468.4 },
            bounded: { mode: 'bounded_after_fix', inputEvents: 2400, expectedCrossings: 2400, persistedCount: 2400, durationMs: 170, throughputEventsPerSec: 13932.8, maxQueueDepth: 256, peakHeapMb: 3.99, totalGcPauseMs: 0, p50Ms: 2, p95Ms: 8, p99Ms: 20.3 },
          });
        }
        if (url.includes('/evidence/chaos')) {
          return ok({ runPrefix: 'run_chaos', total: 1, passed: 1, scenarios: [{ scenario: '5_crash_step5_to_6_replay', expectedCategory: 'RECOVERED_IDEMPOTENT', pgRows: 1, outboxPending: 1, redisSeq: 1, reconcilePass: true, status: 'PASS', detail: 'OK' }] });
        }
        if (url.includes('/reconciliation')) {
          return ok({ runId: 'run-local-001', generated: 18000, brokerAcked: 18000, consumed: 18000, acceptedUnique: 18000, duplicates: 0, invalid: 0, late: 0, gaps: 0, transitionsPersisted: 600, outboxPending: 0, outboxPublished: 600, activeRedisDevices: 1500, lastCommittedOffsets: [], pass: true, limitations: [], checkedAt: '2026-11-22T03:00:15Z' });
        }
        if (url.includes('/api/v1/runs')) return ok(mockRuns);
        return ok({});
      }),
    );
  });

  it('renders honest unavailable Prometheus banner, named p99 boundary, map table alternative, backpressure, and transitions inspector', async () => {
    const user = userEvent.setup();
    render(<App />);

    await waitFor(() => expect(screen.getByText('1498.5 /s')).toBeInTheDocument());
    expect(screen.getByText(/Latency p99 \(INGEST_INTERNAL\)/i)).toBeInTheDocument();
    expect(screen.getByText(/Metrics temporarily unavailable \(no fabricated fallback points\)/i)).toBeInTheDocument();

    // Navigate to Map & Replay -> checks stale freshness + accessible table
    await user.click(screen.getByRole('link', { name: /Map & replay/i }));
    await waitFor(() => expect(screen.getByText(/Position freshness: stale/i)).toBeInTheDocument());
    expect(screen.getByText(/Sampled courier positions/i)).toBeInTheDocument();

    // Navigate to Diagnostics -> checks real backpressure values without fake hardcoded 22%
    await user.click(screen.getByRole('link', { name: /Diagnostics/i }));
    await waitFor(() => expect(screen.getByText(/98.0%/)).toBeInTheDocument());

    // Navigate to Transitions -> inspect detail drawer
    await user.click(screen.getByRole('link', { name: /Transitions/i }));
    await waitFor(() => expect(screen.getByRole('button', { name: 'Inspect' })).toBeInTheDocument());
    await user.click(screen.getByRole('button', { name: 'Inspect' }));
    expect(screen.getByText(/Source Event ID/i)).toBeInTheDocument();

    // Navigate to Run Evidence -> checks reconciliation PASS & chaos matrix
    await user.click(screen.getByRole('link', { name: /Run evidence/i }));
    await waitFor(() => expect(screen.getByText(/RECONCILIATION PASS/i)).toBeInTheDocument());
    expect(screen.getByText(/5_crash_step5_to_6_replay/)).toBeInTheDocument();
  });
});

