package signal

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/model"
)

// VesselStatus is the per-vessel publication state shown to operators.
type VesselStatus string

const (
	// VesselPending waits for its publication attempt.
	VesselPending VesselStatus = "pending"
	// VesselPublishing is the current vessel being written.
	VesselPublishing VesselStatus = "publishing"
	// VesselPublished has both field state and a durable event.
	VesselPublished VesselStatus = "published"
	// VesselFailed failed its write and the publication needs reconciliation.
	VesselFailed VesselStatus = "failed"
	// VesselCompensated had its field signal rolled back after a failed publication.
	VesselCompensated VesselStatus = "compensated"
)

// PublicationStatus is the plan-level outcome.
type PublicationStatus string

const (
	PublicationRunning     PublicationStatus = "running"
	PublicationCompleted   PublicationStatus = "completed"
	PublicationFailed      PublicationStatus = "failed"
	PublicationCompensated PublicationStatus = "compensated"
)

// VesselProgress tracks one vessel inside a publication attempt.
type VesselProgress struct {
	VesselID  uuid.UUID          `json:"vessel_id"`
	Name      string             `json:"name"`
	CallSign  string             `json:"call_sign,omitempty"`
	Status    VesselStatus       `json:"status"`
	Aspect    model.SignalAspect `json:"aspect,omitempty"`
	Attempts  int                `json:"attempts"`
	Error     string             `json:"error,omitempty"`
	UpdatedAt time.Time          `json:"updated_at"`
}

// PublicationRecord is the idempotent answer for one plan's publish request.
// Every repeated request for the same plan observes this same record.
type PublicationRecord struct {
	PlanID     uuid.UUID          `json:"plan_id"`
	ChamberID  string             `json:"chamber_id"`
	CycleID    uuid.UUID          `json:"cycle_id"`
	Generation uint64             `json:"generation"`
	Aspect     model.SignalAspect `json:"aspect"`
	Status     PublicationStatus  `json:"status"`
	Vessels    []VesselProgress   `json:"vessels"`
	Published  []uuid.UUID        `json:"published"`
	StartedAt  time.Time          `json:"started_at"`
	UpdatedAt  time.Time          `json:"updated_at"`
	FinishedAt *time.Time         `json:"finished_at,omitempty"`
	Error      string             `json:"error,omitempty"`

	// previous holds the pre-publication field states used for compensation.
	previous map[uuid.UUID]model.SignalState
	// done is closed once the publication reaches a terminal status so that
	// concurrent duplicate requests wait for the single authoritative result.
	done chan struct{}
}

// Result converts the internal record into the API-facing Publication value.
func (r *PublicationRecord) Result() Publication {
	return Publication{
		CycleID: r.CycleID, Generation: r.Generation,
		Published: append([]uuid.UUID(nil), r.Published...),
	}
}

func (r *PublicationRecord) clone() PublicationRecord {
	copyOf := *r
	copyOf.Vessels = make([]VesselProgress, len(r.Vessels))
	for index := range r.Vessels {
		copyOf.Vessels[index] = r.Vessels[index]
	}
	copyOf.Published = append([]uuid.UUID(nil), r.Published...)
	return copyOf
}

// ErrPublicationTerminal is returned when a plan already has a settled result.
var ErrPublicationTerminal = errors.New("convoy plan publication already settled")

// ErrPublicationRunning is returned while the first request is still in flight.
var ErrPublicationRunning = errors.New("convoy plan publication is already running")

// Registry deduplicates publish requests per plan and exposes progress.
type Registry struct {
	mu      sync.RWMutex
	records map[uuid.UUID]*PublicationRecord
}

func NewRegistry() *Registry {
	return &Registry{records: make(map[uuid.UUID]*PublicationRecord)}
}

// Begin reserves the publication slot for a plan. A finished record is returned
// unchanged so callers can serve the same result again; a running record is
// reported as a conflict instead of starting a second attempt.
func (g *Registry) Begin(planID, cycleID uuid.UUID, generation uint64, chamberID string, aspect model.SignalAspect, vessels []model.Vessel, now time.Time) (*PublicationRecord, error) {
	stamped := now.UTC()
	g.mu.Lock()
	defer g.mu.Unlock()
	if existing, ok := g.records[planID]; ok {
		if !existing.isTerminal() {
			return existing, ErrPublicationRunning
		}
		return existing, ErrPublicationTerminal
	}
	record := &PublicationRecord{
		PlanID: planID, ChamberID: chamberID, CycleID: cycleID, Generation: generation,
		Aspect: aspect, Status: PublicationRunning,
		Vessels:   make([]VesselProgress, 0, len(vessels)),
		Published: make([]uuid.UUID, 0, len(vessels)),
		previous:  make(map[uuid.UUID]model.SignalState),
		StartedAt: stamped, UpdatedAt: stamped, done: make(chan struct{}),
	}
	for _, vessel := range vessels {
		record.Vessels = append(record.Vessels, VesselProgress{
			VesselID: vessel.ID, Name: vessel.Name, CallSign: vessel.CallSign,
			Status: VesselPending, Aspect: aspect, UpdatedAt: stamped,
		})
	}
	g.records[planID] = record
	return record, nil
}

// Get returns the publication record for a plan.
func (g *Registry) Get(planID uuid.UUID) (PublicationRecord, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	record, ok := g.records[planID]
	if !ok {
		return PublicationRecord{}, false
	}
	return record.clone(), true
}

// ByChamber lists every publication touching a chamber, newest start first.
func (g *Registry) ByChamber(chamberID string) []PublicationRecord {
	g.mu.Lock()
	defer g.mu.Unlock()
	result := make([]PublicationRecord, 0)
	for _, record := range g.records {
		if record.ChamberID == chamberID {
			result = append(result, record.clone())
		}
	}
	return result
}

// progress mutators run under the registry lock; the publisher is the only writer.

// live returns the mutable pointer for a plan, or nil when no record exists.
func (g *Registry) live(planID uuid.UUID) *PublicationRecord {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.records[planID]
}

// markPlanCompensated settles a publication reverted by plan cancellation.
// Only vessels whose field signals were actually rolled back are marked.
func (g *Registry) markPlanCompensated(record *PublicationRecord, compensated map[uuid.UUID]struct{}, now time.Time) {
	g.mu.Lock()
	record.Status = PublicationCompensated
	record.Error = ""
	for index := range record.Vessels {
		if _, ok := compensated[record.Vessels[index].VesselID]; ok {
			record.Vessels[index].Status = VesselCompensated
			record.Vessels[index].Error = ""
		}
	}
	record.UpdatedAt = now.UTC()
	stamped := now.UTC()
	if record.FinishedAt == nil {
		record.FinishedAt = &stamped
	}
	select {
	case <-record.done:
	default:
		close(record.done)
	}
	g.mu.Unlock()
}

func (g *Registry) markVessel(record *PublicationRecord, vesselID uuid.UUID, status VesselStatus, attemptErr error, now time.Time) {
	g.mu.Lock()
	for index := range record.Vessels {
		if record.Vessels[index].VesselID == vesselID {
			record.Vessels[index].Status = status
			// One attempt is counted when the write starts; the outcome
			// transition (published/failed) must not count it twice.
			if status == VesselPublishing {
				record.Vessels[index].Attempts++
			}
			record.Vessels[index].UpdatedAt = now.UTC()
			if attemptErr != nil {
				record.Vessels[index].Error = attemptErr.Error()
			} else {
				record.Vessels[index].Error = ""
			}
			break
		}
	}
	record.UpdatedAt = now.UTC()
	g.mu.Unlock()
}

func (g *Registry) rememberPrevious(record *PublicationRecord, vesselID uuid.UUID, previous model.SignalState) {
	g.mu.Lock()
	record.previous[vesselID] = previous
	g.mu.Unlock()
}

func (g *Registry) markPublished(record *PublicationRecord, vesselID uuid.UUID, now time.Time) {
	g.mu.Lock()
	record.Published = append(record.Published, vesselID)
	record.UpdatedAt = now.UTC()
	g.mu.Unlock()
}

func (g *Registry) markCompensated(record *PublicationRecord, vesselID uuid.UUID, now time.Time) {
	g.mu.Lock()
	for index := range record.Vessels {
		if record.Vessels[index].VesselID == vesselID {
			record.Vessels[index].Status = VesselCompensated
			record.Vessels[index].UpdatedAt = now.UTC()
			break
		}
	}
	record.UpdatedAt = now.UTC()
	g.mu.Unlock()
}

// finish settles the record and releases waiting duplicate requests.
func (g *Registry) finish(record *PublicationRecord, status PublicationStatus, failure error, now time.Time) {
	g.mu.Lock()
	record.Status = status
	record.UpdatedAt = now.UTC()
	stamped := now.UTC()
	record.FinishedAt = &stamped
	if failure != nil {
		record.Error = failure.Error()
	} else {
		record.Error = ""
	}
	done := record.done
	g.mu.Unlock()
	close(done)
}

func (r *PublicationRecord) isTerminal() bool {
	switch r.Status {
	case PublicationCompleted, PublicationFailed, PublicationCompensated:
		return true
	default:
		return false
	}
}

// wait blocks until the record settles or ctx ends.
func (r *PublicationRecord) wait(ctx context.Context) error {
	select {
	case <-r.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
