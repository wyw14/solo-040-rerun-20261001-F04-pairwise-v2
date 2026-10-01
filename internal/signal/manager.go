package signal

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/model"
)

// VesselState is the per-vessel publication phase shown to operators.
type VesselState string

const (
	VesselPending     VesselState = "pending"
	VesselPublishing  VesselState = "publishing"
	VesselPublished   VesselState = "published"
	VesselSkipped     VesselState = "skipped"
	VesselFailed      VesselState = "failed"
	VesselCompensated VesselState = "compensated"
)

// AggregateStatus is the terminal-aware state of one publication attempt.
type AggregateStatus string

const (
	StatusRunning   AggregateStatus = "running"
	StatusSucceeded AggregateStatus = "succeeded"
	StatusFailed    AggregateStatus = "failed"
	StatusCancelled AggregateStatus = "cancelled"
)

var (
	ErrPlanAborted      = errors.New("convoy plan is cancelled")
	ErrIdentityConflict = errors.New("idempotency key belongs to another transit generation")
)

const (
	defaultExecTimeout       = 60 * time.Second
	defaultCompensateTimeout = 30 * time.Second
	maxKeyedAggregates       = 16
)

// Preflight re-resolves the live transit after a publication slot is claimed,
// closing the check-then-run race against cancellation and generation changes.
type Preflight func(ctx context.Context, planID uuid.UUID, chamberID string) (model.TransitCycle, error)

// DispatchRequest identifies one operator publish request for a plan.
type DispatchRequest struct {
	PlanID    uuid.UUID
	Key       string
	ChamberID string
	Vessels   []model.Vessel
	Aspect    model.SignalAspect
}

// VesselProgress is the externally visible per-vessel state.
type VesselProgress struct {
	VesselID  uuid.UUID          `json:"vessel_id"`
	Name      string             `json:"name"`
	CallSign  string             `json:"call_sign"`
	State     VesselState        `json:"state"`
	Aspect    model.SignalAspect `json:"aspect,omitempty"`
	Error     string             `json:"error,omitempty"`
	UpdatedAt time.Time          `json:"updated_at"`
}

// AggregateView is the read-only publication snapshot returned to clients.
type AggregateView struct {
	ID          uuid.UUID          `json:"id"`
	PlanID      uuid.UUID          `json:"plan_id"`
	ChamberID   string             `json:"chamber_id"`
	CycleID     uuid.UUID          `json:"cycle_id"`
	Generation  uint64             `json:"generation"`
	Key         string             `json:"idempotency_key,omitempty"`
	Aspect      model.SignalAspect `json:"aspect"`
	Status      AggregateStatus    `json:"status"`
	Vessels     []VesselProgress   `json:"vessels"`
	Published   int                `json:"published"`
	Skipped     int                `json:"skipped"`
	Failed      int                `json:"failed"`
	Compensated int                `json:"compensated"`
	Error       string             `json:"error,omitempty"`
	StartedAt   time.Time          `json:"started_at"`
	FinishedAt  *time.Time         `json:"finished_at,omitempty"`
}

type aggregateKey struct {
	planID uuid.UUID
	key    string
}

// undo describes how to reverse one vessel publication. External marks
// aspects installed out of band (admission pre-release): failure rollback
// leaves them in place, cancellation removes them fail-closed.
type undo struct {
	vesselID uuid.UUID
	written  model.SignalState
	restore  *model.SignalState
	external bool
}

// aggregate is the internal mutable publication record.
type aggregate struct {
	mu     sync.Mutex
	view   AggregateView
	undos  []undo
	cancel context.CancelFunc
	done   chan struct{}
}

// Manager serializes publications per plan, deduplicates concurrent and
// repeated requests, and is the single owner of publication compensation.
type Manager struct {
	baseCtx     context.Context
	publisher   *Publisher
	preflight   Preflight
	now         func() time.Time
	execTimeout time.Duration

	mu      sync.Mutex
	byKey   map[aggregateKey]*aggregate
	latest  map[uuid.UUID]*aggregate
	running map[uuid.UUID]*aggregate
	aborted map[uuid.UUID]bool
}

func NewManager(baseCtx context.Context, publisher *Publisher, preflight Preflight, now func() time.Time) *Manager {
	if now == nil {
		now = time.Now
	}
	if baseCtx == nil {
		baseCtx = context.Background()
	}
	return &Manager{
		baseCtx: baseCtx, publisher: publisher, preflight: preflight, now: now,
		execTimeout: defaultExecTimeout,
		byKey:       make(map[aggregateKey]*aggregate), latest: make(map[uuid.UUID]*aggregate),
		running: make(map[uuid.UUID]*aggregate), aborted: make(map[uuid.UUID]bool),
	}
}

// Dispatch coalesces concurrent and repeated requests for one plan onto a
// single execution. Replayed is true when a stored aggregate answered.
func (m *Manager) Dispatch(ctx context.Context, req DispatchRequest) (AggregateView, bool, error) {
	live, liveErr := m.preflight(ctx, req.PlanID, req.ChamberID)

	m.mu.Lock()
	if m.aborted[req.PlanID] {
		m.mu.Unlock()
		return AggregateView{}, false, ErrPlanAborted
	}
	// An exact idempotency key always answers from its own stored record.
	if req.Key != "" {
		if existing, ok := m.byKey[aggregateKey{planID: req.PlanID, key: req.Key}]; ok {
			m.mu.Unlock()
			view := m.waitView(ctx, existing)
			if view.Status == StatusSucceeded && liveErr == nil &&
				(view.CycleID != live.ID || view.Generation != live.Generation) {
				return view, true, ErrIdentityConflict
			}
			return view, true, nil
		}
	}
	// Only one execution per plan, regardless of idempotency key.
	if active := m.running[req.PlanID]; active != nil {
		m.mu.Unlock()
		return m.waitView(ctx, active), true, nil
	}
	// A terminal success for the same transit generation is the same result.
	if previous := m.latest[req.PlanID]; previous != nil && previous.status() == StatusSucceeded && liveErr == nil {
		view := m.snapshot(previous)
		if view.CycleID == live.ID && view.Generation == live.Generation {
			m.mu.Unlock()
			return view, true, nil
		}
	}
	if liveErr != nil {
		m.mu.Unlock()
		return AggregateView{}, false, liveErr
	}
	created := m.reserveLocked(req)
	m.mu.Unlock()

	go m.run(created, req)
	return m.waitView(ctx, created), false, nil
}

// reserveLocked creates the running aggregate; caller holds m.mu.
func (m *Manager) reserveLocked(req DispatchRequest) *aggregate {
	m.pruneLocked(req.PlanID)
	now := m.now().UTC()
	a := &aggregate{done: make(chan struct{})}
	a.view = AggregateView{
		ID: uuid.New(), PlanID: req.PlanID, ChamberID: req.ChamberID,
		Key: req.Key, Aspect: req.Aspect, Status: StatusRunning, StartedAt: now,
		Vessels: make([]VesselProgress, 0, len(req.Vessels)),
	}
	for _, vessel := range req.Vessels {
		a.view.Vessels = append(a.view.Vessels, VesselProgress{
			VesselID: vessel.ID, Name: vessel.Name, CallSign: vessel.CallSign,
			State: VesselPending, UpdatedAt: now,
		})
	}
	m.byKey[aggregateKey{planID: req.PlanID, key: req.Key}] = a
	m.latest[req.PlanID] = a
	m.running[req.PlanID] = a
	return a
}

// pruneLocked bounds retained terminal keyed aggregates for one plan.
func (m *Manager) pruneLocked(planID uuid.UUID) {
	cutoff := m.now().Add(-time.Hour)
	var terminalKeys []aggregateKey
	for key, a := range m.byKey {
		if key.planID != planID || key.key == "" {
			continue
		}
		view := m.snapshot(a)
		if view.Status == StatusRunning {
			continue
		}
		if view.StartedAt.Before(cutoff) {
			delete(m.byKey, key)
			continue
		}
		terminalKeys = append(terminalKeys, key)
	}
	for len(terminalKeys) >= maxKeyedAggregates {
		oldest := -1
		for index, key := range terminalKeys {
			view := m.snapshot(m.byKey[key])
			if oldest == -1 || view.StartedAt.Before(m.snapshot(m.byKey[terminalKeys[oldest]]).StartedAt) {
				oldest = index
			}
		}
		delete(m.byKey, terminalKeys[oldest])
		terminalKeys = append(terminalKeys[:oldest], terminalKeys[oldest+1:]...)
	}
}

func (m *Manager) run(a *aggregate, req DispatchRequest) {
	execCtx, cancel := context.WithTimeout(context.WithoutCancel(m.baseCtx), m.execTimeout)
	a.mu.Lock()
	a.cancel = cancel
	a.mu.Unlock()
	defer cancel()

	failure := ""
	defer func() {
		target := StatusSucceeded
		switch {
		case m.isAborted(req.PlanID):
			target = StatusCancelled
		case execCtx.Err() != nil:
			target = StatusFailed
			if failure == "" {
				failure = "publication timed out"
			}
		case failure != "":
			target = StatusFailed
		}
		m.finalize(a, req, target, failure)
	}()

	// Re-resolve the live transit under the execution context after claiming.
	cycle, err := m.preflight(execCtx, req.PlanID, req.ChamberID)
	if err != nil {
		failure = err.Error()
		return
	}
	a.mu.Lock()
	a.view.CycleID = cycle.ID
	a.view.Generation = cycle.Generation
	a.mu.Unlock()

	for index, vessel := range req.Vessels {
		if err := execCtx.Err(); err != nil {
			return
		}
		m.markVessel(a, index, VesselPublishing, "")
		result, writeErr := m.publisher.WriteVessel(execCtx, req.ChamberID, vessel, cycle.ID, cycle.Generation, req.Aspect)
		// A state can be installed even when the journal write fails afterwards;
		// it must enter the undo log so finalize can roll it back.
		if result.State.VesselID == vessel.ID {
			entry := undo{vesselID: vessel.ID, written: result.State, external: result.Skipped}
			if result.Previous != nil {
				previous := *result.Previous
				entry.restore = &previous
			}
			a.mu.Lock()
			a.undos = append(a.undos, entry)
			a.mu.Unlock()
		}
		if writeErr != nil {
			message := writeErr.Error()
			if errors.Is(writeErr, context.Canceled) || errors.Is(writeErr, context.DeadlineExceeded) {
				message = "write interrupted"
			}
			m.markVessel(a, index, VesselFailed, message)
			failure = writeErr.Error()
			return
		}
		if result.Skipped {
			m.markVessel(a, index, VesselSkipped, "")
		} else {
			m.markVessel(a, index, VesselPublished, "")
		}
	}
}

// finalize is the only place a running aggregate becomes terminal. The
// terminal transition and compensation decision happen once per aggregate.
func (m *Manager) finalize(a *aggregate, req DispatchRequest, target AggregateStatus, failure string) {
	a.mu.Lock()
	if a.view.Status != StatusRunning {
		a.mu.Unlock()
		return
	}
	finished := m.now().UTC()
	a.view.Status = target
	a.view.FinishedAt = &finished
	if failure != "" {
		a.view.Error = failure
	}
	allUndos := append([]undo(nil), a.undos...)
	// Failure rollback only reverses what this execution installed; signals
	// admission pre-released stay owned by admission. Cancellation removes all
	// proceed signals for the plan, including external ones.
	undos := allUndos
	if target == StatusFailed {
		undos = undos[:0]
		for _, entry := range allUndos {
			if !entry.external {
				undos = append(undos, entry)
			}
		}
	}
	cycleID, generation := a.view.CycleID, a.view.Generation
	a.mu.Unlock()

	if target != StatusSucceeded {
		m.compensate(req.ChamberID, cycleID, generation, a, undos)
	}
	// Keep the slot claimed until compensation settles so a retry cannot start
	// while signals are being rolled back.
	m.mu.Lock()
	delete(m.running, req.PlanID)
	m.mu.Unlock()
	close(a.done)
}

func (m *Manager) compensate(chamberID string, cycleID uuid.UUID, generation uint64, a *aggregate, undos []undo) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(m.baseCtx), defaultCompensateTimeout)
	defer cancel()
	incomplete := false
	for index := len(undos) - 1; index >= 0; index-- {
		entry := undos[index]
		if err := ctx.Err(); err != nil {
			incomplete = true
			break
		}
		action := Undo{VesselID: entry.vesselID, Written: entry.written}
		if entry.restore != nil {
			restored := *entry.restore
			action.Restore = &restored
		}
		if err := m.publisher.UndoVessel(ctx, chamberID, cycleID, generation, action); err != nil {
			incomplete = true
		}
		a.mu.Lock()
		for vesselIndex := range a.view.Vessels {
			if a.view.Vessels[vesselIndex].VesselID == entry.vesselID {
				a.view.Vessels[vesselIndex].State = VesselCompensated
				a.view.Vessels[vesselIndex].UpdatedAt = m.now().UTC()
			}
		}
		a.mu.Unlock()
	}
	if incomplete {
		a.mu.Lock()
		if a.view.Error == "" {
			a.view.Error = "compensation incomplete for one or more vessels"
		}
		a.mu.Unlock()
	}
}

// Abort cancels any in-flight publication for the plan, joins it, and undoes a
// publication that won the race against cancellation. Safe to call repeatedly.
// The boolean reports whether a publication aggregate existed.
func (m *Manager) Abort(planID uuid.UUID) (AggregateView, bool) {
	m.mu.Lock()
	m.aborted[planID] = true
	a := m.latest[planID]
	m.mu.Unlock()
	if a == nil {
		return AggregateView{}, false
	}

	a.mu.Lock()
	cancelFn := a.cancel
	chamberID := a.view.ChamberID
	a.mu.Unlock()
	if cancelFn != nil {
		cancelFn()
	}
	<-a.done

	a.mu.Lock()
	if a.view.Status == StatusSucceeded {
		finished := m.now().UTC()
		a.view.Status = StatusCancelled
		a.view.FinishedAt = &finished
		undos := append([]undo(nil), a.undos...)
		cycleID, generation := a.view.CycleID, a.view.Generation
		a.mu.Unlock()
		m.compensate(chamberID, cycleID, generation, a, undos)
	} else {
		a.mu.Unlock()
	}
	return m.snapshot(a), true
}

func (m *Manager) isAborted(planID uuid.UUID) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.aborted[planID]
}

// Status returns the most recent publication aggregate for a plan.
func (m *Manager) Status(planID uuid.UUID) (AggregateView, bool) {
	m.mu.Lock()
	a := m.latest[planID]
	m.mu.Unlock()
	if a == nil {
		return AggregateView{}, false
	}
	return m.snapshot(a), true
}

// Latest returns the latest aggregate per plan for plan-list embedding.
func (m *Manager) Latest() map[uuid.UUID]AggregateView {
	m.mu.Lock()
	aggregates := make([]*aggregate, 0, len(m.latest))
	for _, a := range m.latest {
		aggregates = append(aggregates, a)
	}
	m.mu.Unlock()
	result := make(map[uuid.UUID]AggregateView, len(aggregates))
	for _, a := range aggregates {
		view := m.snapshot(a)
		result[view.PlanID] = view
	}
	return result
}

func (m *Manager) waitView(ctx context.Context, a *aggregate) AggregateView {
	if a.status() == StatusRunning {
		select {
		case <-a.done:
		case <-ctx.Done():
		}
	}
	return m.snapshot(a)
}

func (m *Manager) snapshot(a *aggregate) AggregateView {
	a.mu.Lock()
	defer a.mu.Unlock()
	view := a.view
	view.Vessels = append([]VesselProgress(nil), a.view.Vessels...)
	for _, vessel := range view.Vessels {
		switch vessel.State {
		case VesselPublished:
			view.Published++
		case VesselSkipped:
			view.Skipped++
		case VesselFailed:
			view.Failed++
		case VesselCompensated:
			view.Compensated++
		}
	}
	return view
}

func (a *aggregate) status() AggregateStatus {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.view.Status
}

func (m *Manager) markVessel(a *aggregate, index int, state VesselState, errMsg string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if index < 0 || index >= len(a.view.Vessels) {
		return
	}
	a.view.Vessels[index].State = state
	a.view.Vessels[index].UpdatedAt = m.now().UTC()
	if state == VesselPublishing || state == VesselPublished || state == VesselSkipped {
		a.view.Vessels[index].Aspect = a.view.Aspect
	}
	if errMsg != "" {
		a.view.Vessels[index].Error = errMsg
	}
}
