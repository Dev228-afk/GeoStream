import type { GeofenceTransitionV1 } from './contracts.gen';

export interface RunRecordDto {
  id: string;
  seed: number;
  profile: string;
  startedAt: string;
  endedAt?: string;
  status: string;
  generatedCount: number;
  brokerAckedCount: number;
  configSha256: string;
}

export interface RunSummaryDto {
  run: RunRecordDto;
  generatedPerSec: number;
  brokerAckedPerSec: number;
  consumedPerSec: number;
  acceptedPerSec: number;
  duplicatePerSec: number;
  invalidPerSec: number;
  kafkaLag: number;
  activeDeviceCount: number;
  latency: {
    boundary: string;
    p50Ms: number;
    p95Ms: number;
    p99Ms: number;
  };
  dataFreshnessAt: string;
  mode: 'LIVE' | 'RECORDED';
  metricsStatus: 'ok' | 'recorded' | 'unavailable';
}

export interface DeviceStateDto {
  RunID: string;
  DeviceID: string;
  LastSeq: number;
  LastEventID: string;
  Lat: number;
  Lon: number;
  EventTime: string;
  ObservedAt: string;
  FenceVersion: number;
  InsideFenceIDs: string[];
  PositionFreshness: 'live' | 'stale';
}

export interface GeofenceFeatureDto {
  type: 'Feature';
  id: string;
  properties: { id: string; name: string; version: number; enabled: boolean };
  geometry: {
    type: 'Polygon' | 'MultiPolygon';
    coordinates: number[][][] | number[][][][];
  };
}

export interface GeofencesCollectionDto {
  type: 'FeatureCollection';
  features: GeofenceFeatureDto[];
}

export interface TransitionRowDto {
  transitionId: string;
  runId: string;
  deviceId: string;
  fenceId: string;
  fenceVersion: number;
  type: 'ENTER' | 'EXIT';
  sourceEventId: string;
  sourceSequence: number;
  eventTime: string;
  createdAt: string;
  sourcePartition: number;
  sourceOffset: number;
}

export interface TimeseriesDto {
  metric: string;
  unit: string;
  points: { at: string; value: number | null }[];
  source: string;
  status: 'ok' | 'unavailable';
}

export interface ReconciliationDto {
  runId: string;
  generated: number;
  brokerAcked: number;
  consumed: number;
  acceptedUnique: number;
  duplicates: number;
  invalid: number;
  late: number;
  gaps: number;
  transitionsPersisted: number;
  outboxPending: number;
  outboxPublished: number;
  activeRedisDevices: number;
  lastCommittedOffsets: { partition: number; startOffset: number; endOffset: number; committedOffset: number }[];
  pass: boolean;
  failureReasons?: string[];
  limitations: string[];
  checkedAt: string;
}

export interface IncidentDto {
  id: string;
  runId: string;
  kind: string;
  severity: string;
  firstAt: string;
  lastAt: string;
  evidenceRef: string;
}

export interface HealthDto {
  status: string;
  dependencies: {
    postgres: string;
    redis: string;
    prometheus: string;
  };
  commit: string;
}

export interface BackpressureModeDto {
  mode: string;
  inputEvents: number;
  expectedCrossings: number;
  persistedCount: number;
  durationMs: number;
  throughputEventsPerSec: number;
  maxQueueDepth: number;
  peakHeapMb: number;
  totalGcPauseMs: number;
  p50Ms: number;
  p95Ms: number;
  p99Ms: number;
}

export interface BackpressureComparisonDto {
  runId: string;
  classification: string;
  historicalResumeClaim: string;
  measuredDeltaVsFixedPct: number;
  identicalOutputCounts: boolean;
  persistedCount: number;
  baseline: BackpressureModeDto;
  bounded: BackpressureModeDto;
}

export interface ChaosMatrixDto {
  runPrefix: string;
  total: number;
  passed: number;
  scenarios: {
    scenario: string;
    expectedCategory: string;
    pgRows: number;
    outboxPending: number;
    redisSeq: number;
    reconcilePass: boolean;
    status: string;
    detail: string;
  }[];
}

export type CanonicalTransition = GeofenceTransitionV1;

export class GeoApiError extends Error {
  status: number;
  code: string;
  constructor(status: number, code: string, message: string) {
    super(message);
    this.status = status;
    this.code = code;
  }
}

async function reqJSON<T>(url: string, signal?: AbortSignal): Promise<T> {
  const res = await fetch(url, { signal });
  if (!res.ok) {
    let code = `HTTP_${res.status}`;
    let msg = res.statusText;
    try {
      const parsed = (await res.json()) as { error?: { code?: string; message?: string } };
      if (parsed.error?.code) code = parsed.error.code;
      if (parsed.error?.message) msg = parsed.error.message;
    } catch {
      // ignore non-json error response
    }
    throw new GeoApiError(res.status, code, msg);
  }
  return (await res.json()) as T;
}

export const geoApi = {
  getHealth: (signal?: AbortSignal) => reqJSON<HealthDto>('/api/v1/health', signal),
  listRuns: (limit = 20, signal?: AbortSignal) =>
    reqJSON<{ items: RunRecordDto[]; nextCursor: string | null }>(`/api/v1/runs?limit=${limit}`, signal),
  getRunSummary: (runId: string, signal?: AbortSignal) =>
    reqJSON<RunSummaryDto>(`/api/v1/runs/${encodeURIComponent(runId)}/summary`, signal),
  listDevices: (runId: string, limit = 100, cursor = '', signal?: AbortSignal) =>
    reqJSON<{ items: DeviceStateDto[]; nextCursor: string | null }>(
      `/api/v1/runs/${encodeURIComponent(runId)}/devices?limit=${limit}${cursor ? `&cursor=${encodeURIComponent(cursor)}` : ''}`,
      signal,
    ),
  getDevice: (runId: string, deviceId: string, signal?: AbortSignal) =>
    reqJSON<DeviceStateDto>(`/api/v1/runs/${encodeURIComponent(runId)}/devices/${encodeURIComponent(deviceId)}`, signal),
  getGeofences: (runId: string, signal?: AbortSignal) =>
    reqJSON<GeofencesCollectionDto>(`/api/v1/runs/${encodeURIComponent(runId)}/geofences`, signal),
  listTransitions: (runId: string, typeFilter = '', cursor = '', limit = 50, signal?: AbortSignal) => {
    const q = new URLSearchParams({ limit: String(limit) });
    if (typeFilter) q.set('type', typeFilter);
    if (cursor) q.set('cursor', cursor);
    return reqJSON<{ items: TransitionRowDto[]; nextCursor: string | null }>(
      `/api/v1/runs/${encodeURIComponent(runId)}/transitions?${q.toString()}`,
      signal,
    );
  },
  getTimeseries: (runId: string, metric: string, window = '5m', resolution = '10s', signal?: AbortSignal) =>
    reqJSON<TimeseriesDto>(
      `/api/v1/runs/${encodeURIComponent(runId)}/timeseries?metric=${encodeURIComponent(metric)}&window=${window}&resolution=${resolution}`,
      signal,
    ),
  getReconciliation: (runId: string, signal?: AbortSignal) =>
    reqJSON<ReconciliationDto>(`/api/v1/runs/${encodeURIComponent(runId)}/reconciliation`, signal),
  listIncidents: (runId: string, signal?: AbortSignal) =>
    reqJSON<{ items: IncidentDto[]; nextCursor: string | null }>(`/api/v1/runs/${encodeURIComponent(runId)}/incidents`, signal),
  getBackpressureEvidence: (signal?: AbortSignal) =>
    reqJSON<BackpressureComparisonDto>('/api/v1/evidence/backpressure', signal),
  getChaosEvidence: (signal?: AbortSignal) =>
    reqJSON<ChaosMatrixDto>('/api/v1/evidence/chaos', signal),
  getAlertsEvidence: (signal?: AbortSignal) =>
    reqJSON<Record<string, unknown>>('/api/v1/evidence/alerts', signal),
};

