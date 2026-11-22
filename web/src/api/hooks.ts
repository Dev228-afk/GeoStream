import { useQuery } from '@tanstack/react-query';
import { geoApi } from './client';

export const gsKeys = {
  health: () => ['health'] as const,
  runs: () => ['runs'] as const,
  summary: (runId: string) => ['summary', runId] as const,
  devices: (runId: string) => ['devices', runId] as const,
  geofences: (runId: string) => ['geofences', runId] as const,
  transitions: (runId: string, type: string, cursor: string) => ['transitions', runId, type, cursor] as const,
  timeseries: (runId: string, metric: string) => ['timeseries', runId, metric] as const,
  reconciliation: (runId: string) => ['reconciliation', runId] as const,
  incidents: (runId: string) => ['incidents', runId] as const,
  backpressure: () => ['evidence', 'backpressure'] as const,
  chaos: () => ['evidence', 'chaos'] as const,
  alerts: () => ['evidence', 'alerts'] as const,
};

export function useHealth() {
  return useQuery({
    queryKey: gsKeys.health(),
    queryFn: ({ signal }) => geoApi.getHealth(signal),
    refetchInterval: 10_000,
    retry: 1,
  });
}

export function useRuns() {
  return useQuery({
    queryKey: gsKeys.runs(),
    queryFn: ({ signal }) => geoApi.listRuns(30, signal),
    staleTime: 10_000,
  });
}

export function useRunSummary(runId: string) {
  return useQuery({
    queryKey: gsKeys.summary(runId),
    queryFn: ({ signal }) => geoApi.getRunSummary(runId, signal),
    enabled: Boolean(runId),
  });
}

export function useTimeseries(runId: string, metric: string) {
  return useQuery({
    queryKey: gsKeys.timeseries(runId, metric),
    queryFn: ({ signal }) => geoApi.getTimeseries(runId, metric, '5m', '10s', signal),
    enabled: Boolean(runId),
  });
}

export function useDevices(runId: string) {
  return useQuery({
    queryKey: gsKeys.devices(runId),
    queryFn: ({ signal }) => geoApi.listDevices(runId, 100, '', signal),
    enabled: Boolean(runId),
  });
}

export function useGeofences(runId: string) {
  return useQuery({
    queryKey: gsKeys.geofences(runId),
    queryFn: ({ signal }) => geoApi.getGeofences(runId, signal),
    enabled: Boolean(runId),
    staleTime: 60_000,
  });
}

export function useTransitions(runId: string, typeFilter: string, cursor: string) {
  return useQuery({
    queryKey: gsKeys.transitions(runId, typeFilter, cursor),
    queryFn: ({ signal }) => geoApi.listTransitions(runId, typeFilter, cursor, 50, signal),
    enabled: Boolean(runId),
  });
}

export function useReconciliation(runId: string) {
  return useQuery({
    queryKey: gsKeys.reconciliation(runId),
    queryFn: ({ signal }) => geoApi.getReconciliation(runId, signal),
    enabled: Boolean(runId),
  });
}

export function useIncidents(runId: string) {
  return useQuery({
    queryKey: gsKeys.incidents(runId),
    queryFn: ({ signal }) => geoApi.listIncidents(runId, signal),
    enabled: Boolean(runId),
  });
}

export function useBackpressureEvidence() {
  return useQuery({
    queryKey: gsKeys.backpressure(),
    queryFn: ({ signal }) => geoApi.getBackpressureEvidence(signal),
    staleTime: 60_000,
  });
}

export function useChaosEvidence() {
  return useQuery({
    queryKey: gsKeys.chaos(),
    queryFn: ({ signal }) => geoApi.getChaosEvidence(signal),
    staleTime: 60_000,
  });
}

export function useAlertsEvidence() {
  return useQuery({
    queryKey: gsKeys.alerts(),
    queryFn: ({ signal }) => geoApi.getAlertsEvidence(signal),
    staleTime: 60_000,
  });
}

