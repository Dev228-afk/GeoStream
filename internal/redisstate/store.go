package redisstate

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/example/geostream/internal/geofence"
	"github.com/redis/go-redis/v9"
)

type AdvanceResult string

const (
	ResultAccepted  AdvanceResult = "ACCEPTED"
	ResultDuplicate AdvanceResult = "DUPLICATE"
	ResultStale     AdvanceResult = "STALE"
	ResultConflict  AdvanceResult = "CONFLICT"
)

type LatestState struct {
	RunID            string
	DeviceID         string
	LastSeq          int64
	LastEventID      string
	PayloadHash      string
	Lat              float64
	Lon              float64
	EventTime        string
	ObservedAt       string
	FenceVersion     int
	InsideFenceIDs   []string
	MembershipDigest string
	PositionFreshness string // "live" | "stale"
}

type LatestStateStore interface {
	Read(ctx context.Context, runID, deviceID string) (LatestState, bool, error)
	CompareAdvance(ctx context.Context, next LatestState, minSeqFloor int64) (AdvanceResult, error)
}

const compareAdvanceLua = `
local key = KEYS[1]
local activeKey = KEYS[2]
local nextSeq = tonumber(ARGV[1])
local nextEventId = ARGV[2]
local nextPayloadHash = ARGV[3]
local lat = ARGV[4]
local lon = ARGV[5]
local eventTime = ARGV[6]
local observedAt = ARGV[7]
local fenceVersion = ARGV[8]
local insideIds = ARGV[9]
local digest = ARGV[10]
local ttl = tonumber(ARGV[11])
local deviceId = ARGV[12]
local nowMs = tonumber(ARGV[13])
local maxActive = tonumber(ARGV[14])
local minFloor = tonumber(ARGV[15])

local curSeqRaw = redis.call("HGET", key, "lastSeq")
local curSeq = nil
if curSeqRaw then
  curSeq = tonumber(curSeqRaw)
else
  curSeq = minFloor
end

if curSeq and curSeq > 0 then
  if nextSeq < curSeq then
    return "STALE"
  end
  if nextSeq == curSeq then
    local curEventId = redis.call("HGET", key, "lastEventId")
    local curHash = redis.call("HGET", key, "payloadHash")
    if curEventId == false then
      -- Cold key reconstructed only from PG durable sequence floor
      return "DUPLICATE"
    end
    if curEventId == nextEventId and curHash == nextPayloadHash then
      return "DUPLICATE"
    end
    return "CONFLICT"
  end
end

redis.call("HSET", key,
  "lastSeq", tostring(nextSeq),
  "lastEventId", nextEventId,
  "payloadHash", nextPayloadHash,
  "lat", lat,
  "lon", lon,
  "eventTime", eventTime,
  "observedAt", observedAt,
  "fenceVersion", fenceVersion,
  "insideFenceIds", insideIds,
  "membershipDigest", digest
)
if ttl > 0 then
  redis.call("EXPIRE", key, ttl)
end

redis.call("ZADD", activeKey, nowMs, deviceId)
if maxActive > 0 then
  local card = redis.call("ZCARD", activeKey)
  if card > maxActive then
    redis.call("ZREMRANGEBYRANK", activeKey, 0, card - maxActive - 1)
  end
end
if ttl > 0 then
  redis.call("EXPIRE", activeKey, ttl)
end
return "ACCEPTED"
`

var advanceScript = redis.NewScript(compareAdvanceLua)

type Store struct {
	client    *redis.Client
	ttl       time.Duration
	maxActive int64
}

func NewStore(client *redis.Client, ttl time.Duration) *Store {
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	return &Store{
		client:    client,
		ttl:       ttl,
		maxActive: 50000,
	}
}

func DeviceKey(runID, deviceID string) string {
	return fmt.Sprintf("geo:%s:device:%s", runID, deviceID)
}

func ActiveKey(runID string) string {
	return fmt.Sprintf("geo:%s:active", runID)
}

func (s *Store) Read(ctx context.Context, runID, deviceID string) (LatestState, bool, error) {
	vals, err := s.client.HGetAll(ctx, DeviceKey(runID, deviceID)).Result()
	if err != nil {
		return LatestState{}, false, err
	}
	if len(vals) == 0 {
		return LatestState{RunID: runID, DeviceID: deviceID, PositionFreshness: "stale"}, false, nil
	}
	seq, _ := strconv.ParseInt(vals["lastSeq"], 10, 64)
	lat, _ := strconv.ParseFloat(vals["lat"], 64)
	lon, _ := strconv.ParseFloat(vals["lon"], 64)
	fv, _ := strconv.Atoi(vals["fenceVersion"])
	var inside []string
	if raw := strings.TrimSpace(vals["insideFenceIds"]); raw != "" {
		inside = strings.Split(raw, ",")
	}
	return LatestState{
		RunID:             runID,
		DeviceID:          deviceID,
		LastSeq:           seq,
		LastEventID:       vals["lastEventId"],
		PayloadHash:       vals["payloadHash"],
		Lat:               lat,
		Lon:               lon,
		EventTime:         vals["eventTime"],
		ObservedAt:        vals["observedAt"],
		FenceVersion:      fv,
		InsideFenceIDs:    inside,
		MembershipDigest:  vals["membershipDigest"],
		PositionFreshness: "live",
	}, true, nil
}

func (s *Store) CompareAdvance(ctx context.Context, next LatestState, minSeqFloor int64) (AdvanceResult, error) {
	digest := next.MembershipDigest
	if digest == "" {
		digest = geofence.MembershipDigest(next.InsideFenceIDs)
	}
	observed := next.ObservedAt
	if observed == "" {
		observed = time.Now().UTC().Format(time.RFC3339Nano)
	}
	res, err := advanceScript.Run(
		ctx,
		s.client,
		[]string{DeviceKey(next.RunID, next.DeviceID), ActiveKey(next.RunID)},
		next.LastSeq,
		next.LastEventID,
		next.PayloadHash,
		fmt.Sprintf("%.7f", next.Lat),
		fmt.Sprintf("%.7f", next.Lon),
		next.EventTime,
		observed,
		strconv.Itoa(next.FenceVersion),
		strings.Join(next.InsideFenceIDs, ","),
		digest,
		int64(s.ttl.Seconds()),
		next.DeviceID,
		time.Now().UTC().UnixMilli(),
		s.maxActive,
		minSeqFloor,
	).Text()
	if err != nil {
		return "", err
	}
	return AdvanceResult(res), nil
}

func (s *Store) ActiveDeviceCount(ctx context.Context, runID string) (int64, error) {
	return s.client.ZCard(ctx, ActiveKey(runID)).Result()
}

func (s *Store) ListActiveDevices(ctx context.Context, runID string, offset, count int64) ([]string, error) {
	if count <= 0 {
		count = 100
	}
	return s.client.ZRevRange(ctx, ActiveKey(runID), offset, offset+count-1).Result()
}

func (s *Store) Ping(ctx context.Context) error {
	return s.client.Ping(ctx).Err()
}

