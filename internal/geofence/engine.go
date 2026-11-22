package geofence

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/example/geostream/internal/schema"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
	"github.com/paulmach/orb/planar"
)

const boundaryEpsilon = 1e-10

type FenceMembership struct {
	FenceID           string
	FenceVersion      int
	Inside            bool
	LastTransitionSeq int64
}

type TransitionIntent struct {
	TransitionID string
	FenceID      string
	FenceVersion int
	Type         string // "ENTER" | "EXIT"
	SourceSeq    int64
	EventTime    string
}

type GeofenceEngine interface {
	Evaluate(p schema.LocationEventV1, previous []FenceMembership) ([]TransitionIntent, error)
}

type Fence struct {
	ID       string
	Version  int
	Name     string
	Enabled  bool
	Bound    orb.Bound
	Polygons []orb.Polygon
	RawGeo   json.RawMessage
}

type pendingCandidate struct {
	targetInside bool
	firstSeen    time.Time
}

type Engine struct {
	fences  []Fence
	dwellMS int64
	mu      sync.Mutex
	pending map[string]pendingCandidate
}

func NewEngine(fences []Fence, dwellMS int64) (*Engine, error) {
	sorted := make([]Fence, len(fences))
	copy(sorted, fences)
	for i := range sorted {
		if sorted[i].ID == "" || sorted[i].Version < 1 || len(sorted[i].Polygons) == 0 {
			return nil, fmt.Errorf("%s: invalid fence %q v%d", schema.ErrFenceGeometryError, sorted[i].ID, sorted[i].Version)
		}
		var b orb.Bound
		for idx, poly := range sorted[i].Polygons {
			pb := poly.Bound()
			if idx == 0 {
				b = pb
			} else {
				b = b.Union(pb)
			}
		}
		sorted[i].Bound = b
	}
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].ID == sorted[j].ID {
			return sorted[i].Version < sorted[j].Version
		}
		return sorted[i].ID < sorted[j].ID
	})
	return &Engine{
		fences:  sorted,
		dwellMS: dwellMS,
		pending: make(map[string]pendingCandidate),
	}, nil
}

func ParseGeoJSON(raw []byte) ([]Fence, error) {
	fc, err := geojson.UnmarshalFeatureCollection(raw)
	if err != nil {
		return nil, fmt.Errorf("%s: unmarshal feature collection: %w", schema.ErrFenceGeometryError, err)
	}
	fences := make([]Fence, 0, len(fc.Features))
	for _, feat := range fc.Features {
		id := ""
		if feat.ID != nil {
			id = fmt.Sprintf("%v", feat.ID)
		}
		if id == "" {
			if v, ok := feat.Properties["id"].(string); ok {
				id = v
			}
		}
		version := 1
		if v, ok := feat.Properties["version"].(float64); ok && int(v) >= 1 {
			version = int(v)
		}
		name, _ := feat.Properties["name"].(string)
		enabled := true
		if v, ok := feat.Properties["enabled"].(bool); ok {
			enabled = v
		}
		var polys []orb.Polygon
		switch g := feat.Geometry.(type) {
		case orb.Polygon:
			polys = []orb.Polygon{g}
		case orb.MultiPolygon:
			polys = []orb.Polygon(g)
		default:
			return nil, fmt.Errorf("%s: fence %q has unsupported geometry %T", schema.ErrFenceGeometryError, id, feat.Geometry)
		}
		geomRaw, _ := json.Marshal(feat.Geometry)
		fences = append(fences, Fence{
			ID:       id,
			Version:  version,
			Name:     name,
			Enabled:  enabled,
			Polygons: polys,
			RawGeo:   geomRaw,
		})
	}
	return fences, nil
}

func pointOnSegment(p, a, b orb.Point) bool {
	dx := b[0] - a[0]
	dy := b[1] - a[1]
	lenSq := dx*dx + dy*dy
	if lenSq == 0 {
		return math.Hypot(p[0]-a[0], p[1]-a[1]) <= boundaryEpsilon
	}
	t := ((p[0]-a[0])*dx + (p[1]-a[1])*dy) / lenSq
	if t < 0 {
		t = 0
	} else if t > 1 {
		t = 1
	}
	projX := a[0] + t*dx
	projY := a[1] + t*dy
	return math.Hypot(p[0]-projX, p[1]-projY) <= boundaryEpsilon
}

func pointOnRingBoundary(ring orb.Ring, pt orb.Point) bool {
	if len(ring) < 2 {
		return false
	}
	for i := 0; i < len(ring)-1; i++ {
		if pointOnSegment(pt, ring[i], ring[i+1]) {
			return true
		}
	}
	if ring[len(ring)-1] != ring[0] {
		if pointOnSegment(pt, ring[len(ring)-1], ring[0]) {
			return true
		}
	}
	return false
}

// polygonContainsWithBoundary implements GeoJSON Polygon semantics:
// outer ring boundary counts as INSIDE; interior rings (holes) are OUTSIDE.
func polygonContainsWithBoundary(poly orb.Polygon, pt orb.Point) bool {
	if len(poly) == 0 {
		return false
	}
	outer := poly[0]
	onOuterBoundary := pointOnRingBoundary(outer, pt)
	inOuter := onOuterBoundary || planar.RingContains(outer, pt)
	if !inOuter {
		return false
	}
	for _, hole := range poly[1:] {
		if planar.RingContains(hole, pt) && !pointOnRingBoundary(hole, pt) {
			return false
		}
	}
	return true
}

func (f Fence) Contains(lon, lat float64) bool {
	if !f.Enabled {
		return false
	}
	pt := orb.Point{lon, lat}
	if !f.Bound.Contains(pt) {
		// Expand check slightly for boundary points coinciding with bbox edges
		if lon < f.Bound.Min[0]-boundaryEpsilon || lon > f.Bound.Max[0]+boundaryEpsilon ||
			lat < f.Bound.Min[1]-boundaryEpsilon || lat > f.Bound.Max[1]+boundaryEpsilon {
			return false
		}
	}
	for _, poly := range f.Polygons {
		if polygonContainsWithBoundary(poly, pt) {
			return true
		}
	}
	return false
}

func (e *Engine) Fences() []Fence {
	out := make([]Fence, len(e.fences))
	copy(out, e.fences)
	return out
}

func (e *Engine) InsideFenceIDs(lon, lat float64) []string {
	var ids []string
	for _, f := range e.fences {
		if f.Contains(lon, lat) {
			ids = append(ids, f.ID)
		}
	}
	return ids
}

func (e *Engine) Evaluate(p schema.LocationEventV1, previous []FenceMembership) ([]TransitionIntent, error) {
	prevMap := make(map[string]FenceMembership, len(previous))
	for _, m := range previous {
		key := fmt.Sprintf("%s#%d", m.FenceID, m.FenceVersion)
		prevMap[key] = m
	}

	evTime, err := time.Parse(time.RFC3339Nano, p.EventTime)
	if err != nil {
		return nil, fmt.Errorf("%s: invalid eventTime: %w", schema.ErrInvalidEvent, err)
	}

	var intents []TransitionIntent
	for _, f := range e.fences {
		if !f.Enabled {
			continue
		}
		key := fmt.Sprintf("%s#%d", f.ID, f.Version)
		wasInside := prevMap[key].Inside
		nowInside := f.Contains(p.Longitude, p.Latitude)

		if nowInside == wasInside {
			if e.dwellMS > 0 {
				e.clearPending(p.RunId, p.DeviceId, f.ID)
			}
			continue
		}

		if e.dwellMS > 0 && !e.confirmDwell(p.RunId, p.DeviceId, f.ID, nowInside, evTime) {
			continue
		}

		typ := "ENTER"
		if !nowInside {
			typ = "EXIT"
		}
		intents = append(intents, TransitionIntent{
			TransitionID: schema.GenerateTransitionID(p.RunId, p.DeviceId, f.ID, f.Version, p.Sequence, typ),
			FenceID:      f.ID,
			FenceVersion: f.Version,
			Type:         typ,
			SourceSeq:    p.Sequence,
			EventTime:    p.EventTime,
		})
	}
	return intents, nil
}

func (e *Engine) clearPending(runID, deviceID, fenceID string) {
	pKey := runID + "|" + deviceID + "|" + fenceID
	e.mu.Lock()
	delete(e.pending, pKey)
	e.mu.Unlock()
}

func (e *Engine) confirmDwell(runID, deviceID, fenceID string, target bool, evTime time.Time) bool {
	pKey := runID + "|" + deviceID + "|" + fenceID
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.pending) > 100000 {
		// Guard memory footprint under stress: evict expired entries cleanly
		cutoff := evTime.Add(-time.Duration(e.dwellMS*2) * time.Millisecond)
		for k, v := range e.pending {
			if v.firstSeen.Before(cutoff) {
				delete(e.pending, k)
			}
		}
	}
	cur, ok := e.pending[pKey]
	if !ok || cur.targetInside != target {
		e.pending[pKey] = pendingCandidate{targetInside: target, firstSeen: evTime}
		return false
	}
	if evTime.Sub(cur.firstSeen) >= time.Duration(e.dwellMS)*time.Millisecond {
		delete(e.pending, pKey)
		return true
	}
	return false
}

func MembershipDigest(insideFenceIDs []string) string {
	if len(insideFenceIDs) == 0 {
		return "none"
	}
	sorted := make([]string, len(insideFenceIDs))
	copy(sorted, insideFenceIDs)
	sort.Strings(sorted)
	sum := sha256.Sum256([]byte(strings.Join(sorted, ",")))
	return hex.EncodeToString(sum[:8])
}

