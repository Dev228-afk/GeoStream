package schema

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/google/uuid"
)

type ErrorCode string

const (
	ErrInvalidEvent       ErrorCode = "INVALID_EVENT"
	ErrDuplicate          ErrorCode = "DUPLICATE"
	ErrStale              ErrorCode = "STALE"
	ErrConflict           ErrorCode = "CONFLICT"
	ErrTransientRedis     ErrorCode = "TRANSIENT_REDIS"
	ErrTransientDB        ErrorCode = "TRANSIENT_DB"
	ErrTransientKafka     ErrorCode = "TRANSIENT_KAFKA"
	ErrFenceGeometryError ErrorCode = "FENCE_GEOMETRY_ERROR"
)

var eventNamespace = uuid.MustParse("6ba7b810-9dad-11d1-80b4-00c04fd430c8")

type ValidationError struct {
	Code    ErrorCode
	Message string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func GenerateEventID(runID, deviceID string, seq int64, seed int64) string {
	key := fmt.Sprintf("geostream:v1:%s:%s:%d:%d", runID, deviceID, seq, seed)
	return uuid.NewSHA1(eventNamespace, []byte(key)).String()
}

func GenerateTransitionID(runID, deviceID, fenceID string, fenceVersion int, seq int64, typ string) string {
	key := fmt.Sprintf("%s|%s|%s|%d|%d|%s", runID, deviceID, fenceID, fenceVersion, seq, typ)
	sum := sha256.Sum256([]byte(key))
	return "tr_" + hex.EncodeToString(sum[:8])
}

func RedactedPayloadHash(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func CanonicalEventPayloadHash(ev LocationEventV1) string {
	canon := fmt.Sprintf("%s|%s|%d|%s|%s|%.7f|%.7f|%.3f",
		ev.RunId, ev.DeviceId, ev.Sequence, ev.RouteId, ev.EventTime, ev.Latitude, ev.Longitude, ev.AccuracyM)
	sum := sha256.Sum256([]byte(canon))
	return hex.EncodeToString(sum[:16])
}

func ValidateLocationEvent(raw []byte, maxFutureSkew time.Duration, now time.Time) (LocationEventV1, error) {
	var ev LocationEventV1
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&ev); err != nil {
		return ev, &ValidationError{Code: ErrInvalidEvent, Message: "malformed json or unknown field: " + err.Error()}
	}
	if dec.More() {
		return ev, &ValidationError{Code: ErrInvalidEvent, Message: "trailing tokens after JSON object"}
	}
	if err := ValidateLocationEventStruct(ev, maxFutureSkew, now); err != nil {
		return ev, err
	}
	return ev, nil
}

func ValidateLocationEventStruct(ev LocationEventV1, maxFutureSkew time.Duration, now time.Time) error {
	if ev.SchemaVersion != 1 {
		return &ValidationError{Code: ErrInvalidEvent, Message: "schemaVersion must equal 1"}
	}
	if strings.TrimSpace(ev.RunId) == "" || len(ev.RunId) > 64 {
		return &ValidationError{Code: ErrInvalidEvent, Message: "invalid runId"}
	}
	if strings.TrimSpace(ev.EventId) == "" || len(ev.EventId) > 64 {
		return &ValidationError{Code: ErrInvalidEvent, Message: "invalid eventId"}
	}
	if strings.TrimSpace(ev.DeviceId) == "" || len(ev.DeviceId) > 64 {
		return &ValidationError{Code: ErrInvalidEvent, Message: "invalid deviceId"}
	}
	if strings.TrimSpace(ev.RouteId) == "" || len(ev.RouteId) > 64 {
		return &ValidationError{Code: ErrInvalidEvent, Message: "invalid routeId"}
	}
	if ev.Sequence < 1 {
		return &ValidationError{Code: ErrInvalidEvent, Message: "sequence must be >= 1"}
	}
	if math.IsNaN(ev.Latitude) || math.IsInf(ev.Latitude, 0) || ev.Latitude < -90 || ev.Latitude > 90 {
		return &ValidationError{Code: ErrInvalidEvent, Message: "latitude out of [-90,90]"}
	}
	if math.IsNaN(ev.Longitude) || math.IsInf(ev.Longitude, 0) || ev.Longitude < -180 || ev.Longitude > 180 {
		return &ValidationError{Code: ErrInvalidEvent, Message: "longitude out of [-180,180]"}
	}
	if math.IsNaN(ev.AccuracyM) || math.IsInf(ev.AccuracyM, 0) || ev.AccuracyM < 0 || ev.AccuracyM > 200 {
		return &ValidationError{Code: ErrInvalidEvent, Message: "accuracyM out of [0,200]"}
	}
	evTime, err := time.Parse(time.RFC3339Nano, ev.EventTime)
	if err != nil {
		return &ValidationError{Code: ErrInvalidEvent, Message: "invalid eventTime RFC3339"}
	}
	if _, err := time.Parse(time.RFC3339Nano, ev.ProducedAt); err != nil {
		return &ValidationError{Code: ErrInvalidEvent, Message: "invalid producedAt RFC3339"}
	}
	if !now.IsZero() && maxFutureSkew > 0 && evTime.After(now.Add(maxFutureSkew)) {
		return &ValidationError{Code: ErrInvalidEvent, Message: "eventTime exceeds allowed future skew"}
	}
	return nil
}

