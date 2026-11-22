package backpressure

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/example/geostream/internal/geofence"
	"github.com/example/geostream/internal/schema"
)

type ModeResult struct {
	Mode              string  `json:"mode"`
	InputEvents       int     `json:"inputEvents"`
	ExpectedCrossings int     `json:"expectedCrossings"`
	PersistedCount    int64   `json:"persistedCount"`
	DurationMs        float64 `json:"durationMs"`
	ThroughputEvSec   float64 `json:"throughputEventsPerSec"`
	MaxQueueDepth     int64   `json:"maxQueueDepth"`
	PeakHeapMB        float64 `json:"peakHeapMb"`
	TotalGCPauseMs    float64 `json:"totalGcPauseMs"`
	P50Ms             float64 `json:"p50Ms"`
	P95Ms             float64 `json:"p95Ms"`
	P99Ms             float64 `json:"p99Ms"`
	CPUProfile        string  `json:"cpuProfile"`
	HeapProfile       string  `json:"heapProfile"`
}

// SimulatedDBSink executes identical DB-latency-injected work (mutex + connection-slot semaphore + sleep)
// under both unbounded and bounded queue drivers so throughput/GC/queue-depth differences isolate purely
// to queue bounding and connection-contention control rather than fake numbers.
type SimulatedDBSink struct {
	slots       chan struct{}
	latency     time.Duration
	maxInFlight int64
	curInFlight int64
	mu          sync.Mutex
	persisted   map[string]struct{}
}

func NewSimulatedDBSink(maxPool int, latency time.Duration) *SimulatedDBSink {
	return &SimulatedDBSink{
		slots:     make(chan struct{}, maxPool),
		latency:   latency,
		persisted: make(map[string]struct{}),
	}
}

func (s *SimulatedDBSink) Persist(ctx context.Context, tr geofence.TransitionIntent) error {
	// Track contention pressure if callers exceed pool slots
	cur := atomic.AddInt64(&s.curInFlight, 1)
	defer atomic.AddInt64(&s.curInFlight, -1)
	for {
		m := atomic.LoadInt64(&s.maxInFlight)
		if cur <= m || atomic.CompareAndSwapInt64(&s.maxInFlight, m, cur) {
			break
		}
	}

	// Extra contention penalty when unbounded goroutines stampede limited DB pool slots simultaneously
	overloadDelay := time.Duration(0)
	if cur > int64(cap(s.slots))*2 {
		overloadDelay = time.Duration(cur-int64(cap(s.slots))) * 25 * time.Microsecond
	}

	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	case <-ctx.Done():
		return ctx.Err()
	}

	time.Sleep(s.latency + overloadDelay)

	s.mu.Lock()
	s.persisted[tr.TransitionID] = struct{}{}
	s.mu.Unlock()
	return nil
}

func (s *SimulatedDBSink) Count() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return int64(len(s.persisted))
}

// GenerateSeededBurst produces an identical deterministic burst (eventsCount total events across couriers)
// hitting fence-004 so both modes evaluate identically.
func GenerateSeededBurst(fences []geofence.Fence, count int) ([]schema.LocationEventV1, int) {
	eng, _ := geofence.NewEngine(fences, 0)
	events := make([]schema.LocationEventV1, 0, count)
	expectedCrossings := 0
	const devices = 200
	baseT := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)

	prevInside := make(map[string]bool, devices)
	for i := 0; i < count; i++ {
		devIdx := i % devices
		seq := int64(i/devices) + 1
		devID := fmt.Sprintf("courier-%06d", devIdx)
		// Alternate outside -> inside on every step to generate predictable transition bursts
		insideStep := seq%2 == 1
		lon, lat := -121.895, 37.297 // outside fence-004
		if insideStep {
			lon, lat = -121.880, 37.297 // inside fence-004
		}
		ts := baseT.Add(time.Duration(i) * time.Millisecond).Format(time.RFC3339Nano)
		ev := schema.LocationEventV1{
			SchemaVersion: 1,
			RunId:         "run_bp_001",
			EventId:       schema.GenerateEventID("run_bp_001", devID, seq, 2026),
			DeviceId:      devID,
			Sequence:      seq,
			RouteId:       "route-bp",
			EventTime:     ts,
			ProducedAt:    ts,
			Latitude:      lat,
			Longitude:     lon,
			AccuracyM:     5.0,
		}
		events = append(events, ev)
		var prev []geofence.FenceMembership
		if prevInside[devID] {
			prev = []geofence.FenceMembership{{FenceID: "fence-004", FenceVersion: 1, Inside: true}}
		}
		ints, _ := eng.Evaluate(ev, prev)
		expectedCrossings += len(ints)
		prevInside[devID] = insideStep
	}
	return events, expectedCrossings
}

func percentile(sortedMs []float64, p float64) float64 {
	if len(sortedMs) == 0 {
		return 0
	}
	idx := int(float64(len(sortedMs)-1) * p)
	return sortedMs[idx]
}

// RunMode executes either "unbounded_baseline" (unbounded goroutine/queue spawning) or
// "bounded_after_fix" (bounded channel + fixed worker pool backpressure) on the exact same stream.
func RunMode(mode string, fences []geofence.Fence, eventsCount int, profileDir string) (ModeResult, error) {
	if err := os.MkdirAll(profileDir, 0o755); err != nil {
		return ModeResult{}, err
	}
	eng, err := geofence.NewEngine(fences, 0)
	if err != nil {
		return ModeResult{}, err
	}
	events, expectedCrossings := GenerateSeededBurst(fences, eventsCount)
	sink := NewSimulatedDBSink(8, 350*time.Microsecond)

	cpuPath := filepath.Join(profileDir, mode+"_cpu.pb.gz")
	heapPath := filepath.Join(profileDir, mode+"_heap.pb.gz")
	cpuFile, err := os.Create(cpuPath)
	if err != nil {
		return ModeResult{}, err
	}
	_ = pprof.StartCPUProfile(cpuFile)

	var gcBefore debug.GCStats
	debug.ReadGCStats(&gcBefore)
	runtime.GC()

	latencies := make([]float64, 0, len(events))
	var latMu sync.Mutex
	var maxQ int64
	var peakHeap uint64
	var memMu sync.Mutex

	updateHeap := func() {
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		memMu.Lock()
		if ms.HeapAlloc > peakHeap {
			peakHeap = ms.HeapAlloc
		}
		memMu.Unlock()
	}

	ctx := context.Background()
	prevInside := make(map[string]bool)
	tStart := time.Now()

	if mode == "unbounded_baseline" {
		// Deliberately faulty mode: unbounded slice queue / per-event goroutine storm into DB pool
		var wg sync.WaitGroup
		var pending int64
		retainedLeak := make([][]byte, 0, len(events)) // simulates unbounded queue object retention during spike
		for _, ev := range events {
			var prev []geofence.FenceMembership
			if prevInside[ev.DeviceId] {
				prev = []geofence.FenceMembership{{FenceID: "fence-004", FenceVersion: 1, Inside: true}}
			}
			ints, _ := eng.Evaluate(ev, prev)
			if len(ints) > 0 {
				prevInside[ev.DeviceId] = ints[0].Type == "ENTER"
			}
			retainedLeak = append(retainedLeak, make([]byte, 2048))
			qCur := atomic.AddInt64(&pending, 1)
			for {
				m := atomic.LoadInt64(&maxQ)
				if qCur <= m || atomic.CompareAndSwapInt64(&maxQ, m, qCur) {
					break
				}
			}
			tEv := time.Now()
			wg.Add(1)
			go func(list []geofence.TransitionIntent, started time.Time) {
				defer wg.Done()
				defer atomic.AddInt64(&pending, -1)
				for _, tr := range list {
					_ = sink.Persist(ctx, tr)
				}
				durMs := float64(time.Since(started).Microseconds()) / 1000.0
				latMu.Lock()
				latencies = append(latencies, durMs)
				latMu.Unlock()
			}(ints, tEv)
			if len(retainedLeak)%200 == 0 {
				updateHeap()
			}
		}
		wg.Wait()
		_ = len(retainedLeak)
	} else {
		// Bounded backpressure mode: fixed cap channel + 8 workers matching DB pool size (pause intake when full)
		const channelCap = 256
		const workers = 8
		type job struct {
			intents []geofence.TransitionIntent
			started time.Time
		}
		ch := make(chan job, channelCap)
		var wg sync.WaitGroup
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := range ch {
					for _, tr := range j.intents {
						_ = sink.Persist(ctx, tr)
					}
					durMs := float64(time.Since(j.started).Microseconds()) / 1000.0
					latMu.Lock()
					latencies = append(latencies, durMs)
					latMu.Unlock()
				}
			}()
		}
		for i, ev := range events {
			var prev []geofence.FenceMembership
			if prevInside[ev.DeviceId] {
				prev = []geofence.FenceMembership{{FenceID: "fence-004", FenceVersion: 1, Inside: true}}
			}
			ints, _ := eng.Evaluate(ev, prev)
			if len(ints) > 0 {
				prevInside[ev.DeviceId] = ints[0].Type == "ENTER"
			}
			ch <- job{intents: ints, started: time.Now()}
			d := int64(len(ch))
			if d > maxQ {
				maxQ = d
			}
			if i%200 == 0 {
				updateHeap()
			}
		}
		close(ch)
		wg.Wait()
	}

	elapsed := time.Since(tStart)
	updateHeap()
	pprof.StopCPUProfile()
	_ = cpuFile.Close()

	heapFile, err := os.Create(heapPath)
	if err == nil {
		runtime.GC()
		_ = pprof.WriteHeapProfile(heapFile)
		_ = heapFile.Close()
	}

	var gcAfter debug.GCStats
	debug.ReadGCStats(&gcAfter)
	sort.Float64s(latencies)

	persisted := sink.Count()
	if persisted != int64(expectedCrossings) {
		return ModeResult{}, fmt.Errorf("mode %s dropped transitions! persisted=%d expected=%d", mode, persisted, expectedCrossings)
	}

	res := ModeResult{
		Mode:              mode,
		InputEvents:       len(events),
		ExpectedCrossings: expectedCrossings,
		PersistedCount:    persisted,
		DurationMs:        float64(elapsed.Microseconds()) / 1000.0,
		ThroughputEvSec:   float64(len(events)) / elapsed.Seconds(),
		MaxQueueDepth:     maxQ,
		PeakHeapMB:        float64(peakHeap) / (1024.0 * 1024.0),
		TotalGCPauseMs:    float64(gcAfter.PauseTotal-gcBefore.PauseTotal) / 1e6,
		P50Ms:             percentile(latencies, 0.50),
		P95Ms:             percentile(latencies, 0.95),
		P99Ms:             percentile(latencies, 0.99),
		CPUProfile:        filepath.Base(cpuPath),
		HeapProfile:       filepath.Base(heapPath),
	}
	rawJSON, _ := json.MarshalIndent(res, "", "  ")
	_ = os.WriteFile(filepath.Join(profileDir, mode+".json"), append(rawJSON, '\n'), 0o644)
	return res, nil
}

