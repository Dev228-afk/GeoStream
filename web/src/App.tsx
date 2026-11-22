import { useEffect, useMemo, useState } from 'react';
import { BrowserRouter, Navigate, Route, Routes, useLocation, useParams } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { useRuns } from './api/hooks';
import { ConsoleShell } from './components/ConsoleShell';
import { DiagnosticsPage } from './routes/DiagnosticsPage';
import { EvidencePage } from './routes/EvidencePage';
import { MapReplayPage } from './routes/MapReplayPage';
import { OverviewPage } from './routes/OverviewPage';
import { TransitionsPage } from './routes/TransitionsPage';

const queryClient = new QueryClient({
  defaultOptions: {
    queries: { staleTime: 5000, refetchOnWindowFocus: false },
  },
});

const ROUTE_META: { match: string; title: string; subtitle: string }[] = [
  { match: '/overview', title: 'Pipeline Overview', subtitle: 'Run ingestion rates, latency boundary metrics, and active incidents' },
  { match: '/map', title: 'Map & Event Replay', subtitle: 'Sampled synthetic courier telemetry (<=100 markers) over GeoJSON fences' },
  { match: '/diagnostics', title: 'Backpressure & Partition Diagnostics', subtitle: 'Controlled bounded vs. unbounded regression proof & Kafka offset lag' },
  { match: '/transitions', title: 'Durable Geofence Transitions', subtitle: 'PostgreSQL cursor-paginated ENTER/EXIT events with Kafka/outbox provenance' },
];

function EvidenceRouteWrapper({ fallbackRunId }: { fallbackRunId: string }) {
  const { id } = useParams<{ id: string }>();
  return <EvidencePage runId={id || fallbackRunId} />;
}

export function GeoStreamAppContent() {
  const runsQ = useRuns();
  const runs = useMemo(() => runsQ.data?.items ?? [], [runsQ.data]);
  const [selectedRunId, setSelectedRunId] = useState<string>('');

  useEffect(() => {
    if (!selectedRunId && runs.length > 0) {
      const pref = runs.find((r) => r.id === 'run-local-001') ?? runs[0];
      setSelectedRunId(pref.id);
    }
  }, [runs, selectedRunId]);

  const activeRunId = selectedRunId || runs[0]?.id || 'run-local-001';
  const { pathname } = useLocation();
  const meta = ROUTE_META.find((m) => pathname.startsWith(m.match)) ?? {
    title: 'Run Evidence & Chaos Verification',
    subtitle: 'Produced-vs-consumed reconciliation, chaos drill outputs, and raw alert proofs',
  };

  return (
    <ConsoleShell
      title={meta.title}
      subtitle={meta.subtitle}
      runs={runs}
      selectedRunId={activeRunId}
      onSelectRun={setSelectedRunId}
    >
      <Routes>
        <Route path="/overview" element={<OverviewPage runId={activeRunId} />} />
        <Route path="/map" element={<MapReplayPage runId={activeRunId} />} />
        <Route path="/diagnostics" element={<DiagnosticsPage runId={activeRunId} />} />
        <Route path="/transitions" element={<TransitionsPage runId={activeRunId} />} />
        <Route path="/runs/:id/evidence" element={<EvidenceRouteWrapper fallbackRunId={activeRunId} />} />
        <Route path="*" element={<Navigate to="/overview" replace />} />
      </Routes>
    </ConsoleShell>
  );
}

export function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <GeoStreamAppContent />
      </BrowserRouter>
    </QueryClientProvider>
  );
}
