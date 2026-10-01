package signal

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
)

type Publication struct {
	CycleID    uuid.UUID                       `json:"cycle_id"`
	Generation uint64                          `json:"generation"`
	Published  []uuid.UUID                     `json:"published"`
	Previous   map[uuid.UUID]model.SignalState `json:"-"`
}

// Publisher applies a convoy signal change vessel by vessel. Every plan keeps
// one authoritative publication record: repeated requests return the same
// result, and a partial write is reconciled by compensation.
type Publisher struct {
	store    *Store
	recorder *journal.Recorder
	progress *Registry
	now      func() time.Time
}

func NewPublisher(store *Store, recorder *journal.Recorder, now func() time.Time) *Publisher {
	if now == nil {
		now = time.Now
	}
	return &Publisher{store: store, recorder: recorder, progress: NewRegistry(), now: now}
}

// Progress exposes the per-plan publication registry used by status endpoints.
func (p *Publisher) Progress() *Registry { return p.progress }

// PublishPlan publishes the plan signals for a transit generation.
//
// The first caller drives the vessel-by-vessel write. Every later request for
// the same plan observes the same authoritative record: an in-flight attempt
// is awaited, a finished attempt (completed, compensated or failed) returns
// its original result unchanged. Recovery after a partial write is handled
// internally by rolling back the vessels that were already written.
func (p *Publisher) PublishPlan(ctx context.Context, plan model.ConvoyPlan, cycle model.TransitCycle, aspect model.SignalAspect) (Publication, PublicationRecord, error) {
	record, err := p.progress.Begin(plan.ID, cycle.ID, cycle.Generation, plan.ChamberID, aspect, plan.Vessels, p.now())
	if err != nil {
		if errors.Is(err, ErrPublicationRunning) {
			if waitErr := record.wait(ctx); waitErr != nil {
				return Publication{}, PublicationRecord{}, waitErr
			}
			snapshot, _ := p.progress.Get(plan.ID)
			return snapshot.Result(), snapshot, terminalError(snapshot)
		}
		if errors.Is(err, ErrPublicationTerminal) {
			return record.Result(), *record, terminalError(*record)
		}
		return Publication{}, PublicationRecord{}, err
	}

	publication, failure := p.run(ctx, plan, cycle, aspect, record)
	if failure == nil {
		p.progress.finish(record, PublicationCompleted, nil, p.now())
		snapshot, _ := p.progress.Get(plan.ID)
		return publication, snapshot, nil
	}

	// The request context may already be cancelled; compensation must still run.
	_, compensationErr := p.compensateRecord(context.WithoutCancel(ctx), record)
	status := PublicationCompensated
	if compensationErr != nil {
		status = PublicationFailed
		failure = errors.Join(failure, errors.New("signal compensation failed"), compensationErr)
	}
	p.progress.finish(record, status, failure, p.now())
	snapshot, _ := p.progress.Get(plan.ID)
	return publication, snapshot, failure
}

// terminalError reconstructs the error carried by a settled publication so a
// repeated request receives the same outcome as the first.
func terminalError(record PublicationRecord) error {
	if record.Status == PublicationCompleted || record.Error == "" {
		return nil
	}
	return errors.New(record.Error)
}

// run writes each vessel sequentially, stamping the owning lock run and
// generation on the field state. Stamped signals are what the handoff fencing
// in the cycle coordinator validates against.
func (p *Publisher) run(ctx context.Context, plan model.ConvoyPlan, cycle model.TransitCycle, aspect model.SignalAspect, record *PublicationRecord) (Publication, error) {
	publication := Publication{
		CycleID: cycle.ID, Generation: cycle.Generation,
		Previous: make(map[uuid.UUID]model.SignalState),
	}
	for _, vessel := range plan.Vessels {
		if err := ctx.Err(); err != nil {
			p.progress.markVessel(record, vessel.ID, VesselFailed, err, p.now())
			return publication, err
		}
		p.progress.markVessel(record, vessel.ID, VesselPublishing, nil, p.now())
		if previous, ok := p.store.Get(vessel.ID); ok {
			publication.Previous[vessel.ID] = previous
			p.progress.rememberPrevious(record, vessel.ID, previous)
		}
		state := p.store.Set(vessel.ID, cycle.ID, cycle.Generation, aspect, p.now())
		if _, err := p.recorder.Record(ctx, plan.ChamberID, model.EventSignalChanged, cycle.ID, cycle.Generation, state); err != nil {
			// The in-memory write was never made durable: undo it directly.
			// Earlier, durable vessels are rolled back by compensateRecord.
			if previous, exists := publication.Previous[vessel.ID]; exists {
				p.store.Set(previous.VesselID, previous.CycleID, previous.Generation, previous.Aspect, p.now())
			} else {
				p.store.Remove(vessel.ID)
			}
			p.progress.markVessel(record, vessel.ID, VesselFailed, err, p.now())
			return publication, err
		}
		publication.Published = append(publication.Published, vessel.ID)
		p.progress.markPublished(record, vessel.ID, p.now())
		p.progress.markVessel(record, vessel.ID, VesselPublished, nil, p.now())
	}
	return publication, nil
}

// compensateRecord rolls back every vessel that was actually written, in
// reverse order. A field signal is touched only while it still carries this
// publication's cycle identity, so a newer lock generation can never be undone.
// It returns the vessel IDs whose signals were actually reverted.
func (p *Publisher) compensateRecord(ctx context.Context, record *PublicationRecord) (map[uuid.UUID]struct{}, error) {
	compensated := make(map[uuid.UUID]struct{})
	var combined error
	for index := len(record.Published) - 1; index >= 0; index-- {
		vesselID := record.Published[index]
		current, ok := p.store.Get(vesselID)
		if ok && !current.IdentityMatches(record.CycleID, record.Generation) {
			// The field now carries a newer lock run/generation: leave it.
			continue
		}
		if previous, exists := record.previous[vesselID]; exists {
			p.store.Set(previous.VesselID, previous.CycleID, previous.Generation, previous.Aspect, p.now())
		} else {
			p.store.Remove(vesselID)
		}
		_, err := p.recorder.Record(ctx, record.ChamberID, model.EventSignalChanged, record.CycleID, record.Generation, map[string]any{
			"plan_id": record.PlanID, "vessel_id": vesselID, "compensated": true,
		})
		if err != nil {
			combined = errors.Join(combined, err)
			continue
		}
		compensated[vesselID] = struct{}{}
		p.progress.markCompensated(record, vesselID, p.now())
	}
	return compensated, combined
}

// CompensatePlan reverts the field signals owned by a plan when it is
// cancelled. Every plan vessel whose signal still carries this transit's
// cycle identity is cleared — including the proceed aspect stamped during
// admission. Signals stamped by other lock runs/generations are left
// untouched so cancellation can never fence a newer transit.
func (p *Publisher) CompensatePlan(ctx context.Context, plan model.ConvoyPlan, cycleID uuid.UUID, generation uint64) (int, error) {
	record := p.progress.live(plan.ID)
	compensated := make(map[uuid.UUID]struct{})
	var combined error
	for _, vessel := range plan.Vessels {
		current, ok := p.store.Get(vessel.ID)
		if !ok {
			continue
		}
		if cycleID != uuid.Nil && !current.IdentityMatches(cycleID, generation) {
			continue
		}
		p.store.Remove(vessel.ID)
		if _, err := p.recorder.Record(ctx, plan.ChamberID, model.EventSignalChanged, cycleID, generation, map[string]any{
			"plan_id": plan.ID, "vessel_id": vessel.ID, "compensated": true,
		}); err != nil {
			combined = errors.Join(combined, err)
			continue
		}
		compensated[vessel.ID] = struct{}{}
		if record != nil {
			p.progress.markCompensated(record, vessel.ID, p.now())
		}
	}
	if combined != nil {
		return 0, combined
	}
	if record != nil {
		p.progress.markPlanCompensated(record, compensated, p.now())
	}
	return len(compensated), nil
}
