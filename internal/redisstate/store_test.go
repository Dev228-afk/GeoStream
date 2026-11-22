package redisstate

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestRedisAtomicLuaCompareAdvanceAndConcurrency(t *testing.T) {
	ctx := context.Background()
	cl := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6383"})
	if err := cl.Ping(ctx).Err(); err != nil {
		t.Fatalf("redis unreachable on 6383: %v", err)
	}
	defer cl.Close()

	runID := "run_test_redis_cas"
	devID := "courier-cas-001"
	cl.Del(ctx, DeviceKey(runID, devID), ActiveKey(runID))

	st := NewStore(cl, time.Hour)

	res1, err := st.CompareAdvance(ctx, LatestState{
		RunID: runID, DeviceID: devID, LastSeq: 2, LastEventID: "ev-2", PayloadHash: "hash-2",
		Lat: 37.30, Lon: -121.88, EventTime: "2026-11-20T12:00:02Z", FenceVersion: 1,
		InsideFenceIDs: []string{"fence-004"},
	}, 0)
	if err != nil || res1 != ResultAccepted {
		t.Fatalf("expected ACCEPTED on seq 2, got %s err=%v", res1, err)
	}

	// Lower sequence -> STALE without overwriting
	resStale, err := st.CompareAdvance(ctx, LatestState{
		RunID: runID, DeviceID: devID, LastSeq: 1, LastEventID: "ev-1", PayloadHash: "hash-1",
		Lat: 37.29, Lon: -121.89, EventTime: "2026-11-20T12:00:01Z", FenceVersion: 1,
	}, 0)
	if err != nil || resStale != ResultStale {
		t.Fatalf("expected STALE on seq 1, got %s err=%v", resStale, err)
	}

	// Equal sequence + identical payload -> DUPLICATE
	resDup, err := st.CompareAdvance(ctx, LatestState{
		RunID: runID, DeviceID: devID, LastSeq: 2, LastEventID: "ev-2", PayloadHash: "hash-2",
		Lat: 37.30, Lon: -121.88, EventTime: "2026-11-20T12:00:02Z", FenceVersion: 1,
	}, 0)
	if err != nil || resDup != ResultDuplicate {
		t.Fatalf("expected DUPLICATE on seq 2, got %s err=%v", resDup, err)
	}

	// Equal sequence + different payload -> CONFLICT
	resConflict, err := st.CompareAdvance(ctx, LatestState{
		RunID: runID, DeviceID: devID, LastSeq: 2, LastEventID: "ev-2-alt", PayloadHash: "hash-different",
		Lat: 37.31, Lon: -121.87, EventTime: "2026-11-20T12:00:02Z", FenceVersion: 1,
	}, 0)
	if err != nil || resConflict != ResultConflict {
		t.Fatalf("expected CONFLICT on eq seq + diff payload, got %s err=%v", resConflict, err)
	}

	// Concurrent race at seq 5: only ONE goroutine gets ACCEPTED; others get DUPLICATE
	var wg sync.WaitGroup
	var acceptedCnt int
	var mu sync.Mutex
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, rErr := st.CompareAdvance(ctx, LatestState{
				RunID: runID, DeviceID: devID, LastSeq: 5, LastEventID: "ev-5", PayloadHash: "hash-5",
				Lat: 37.30, Lon: -121.88, EventTime: "2026-11-20T12:00:05Z", FenceVersion: 1,
			}, 0)
			if rErr != nil {
				t.Errorf("concurrent CAS error: %v", rErr)
				return
			}
			if r == ResultAccepted {
				mu.Lock()
				acceptedCnt++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if acceptedCnt != 1 {
		t.Fatalf("expected exactly 1 ACCEPTED under concurrent CAS at seq 5, got %d", acceptedCnt)
	}

	readBack, ok, err := st.Read(ctx, runID, devID)
	if err != nil || !ok || readBack.LastSeq != 5 || readBack.PositionFreshness != "live" {
		t.Fatalf("unexpected Read after CAS: ok=%v err=%v state=%+v", ok, err, readBack)
	}

	// Missing/evicted key returns PositionFreshness=stale
	missing, ok2, err := st.Read(ctx, runID, "courier-evicted-999")
	if err != nil || ok2 || missing.PositionFreshness != "stale" {
		t.Fatalf("expected cold missing key stale, got ok=%v state=%+v err=%v", ok2, missing, err)
	}
}

