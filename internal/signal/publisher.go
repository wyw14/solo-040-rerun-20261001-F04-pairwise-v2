package signal

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
)

var ErrSignalConflict = errors.New("signal state changed by another transit generation")

// Undo describes how to reverse a single vessel publication. Exactly one
// action applies: Restore (when the vessel previously held another signal)
// or removal keyed by Written (the state this publication installed).
type Undo struct {
	VesselID uuid.UUID
	Restore  *model.SignalState
	Written  model.SignalState
}

// Publisher owns the single-vessel field-signal primitives. The Manager
// orchestrates multi-vessel publications and decides when they are undone.
type Publisher struct {
	store    *Store
	recorder *journal.Recorder
	now      func() time.Time
}

func NewPublisher(store *Store, recorder *journal.Recorder, now func() time.Time) *Publisher {
	if now == nil {
		now = time.Now
	}
	return &Publisher{store: store, recorder: recorder, now: now}
}

// WriteResult reports what one WriteVessel call did.
type WriteResult struct {
	State    model.SignalState
	Previous *model.SignalState
	// Skipped is true when the vessel already carried the identical aspect
	// for this cycle and generation (for example admission pre-released it);
	// no forward journal event was emitted in that case.
	Skipped bool
}

// WriteVessel stamps one vessel for a transit generation with compare-and-set
// fencing. The context is checked on both sides of the store write.
func (p *Publisher) WriteVessel(ctx context.Context, chamberID string, vessel model.Vessel, cycleID uuid.UUID, generation uint64, aspect model.SignalAspect) (WriteResult, error) {
	if err := ctx.Err(); err != nil {
		return WriteResult{}, err
	}
	previous, hadPrevious := p.store.Get(vessel.ID)
	if hadPrevious && previous.CycleID == cycleID && previous.Generation == generation && previous.Aspect == aspect {
		// The identical aspect for this generation was installed out of band
		// (typically admission pre-release); vessel identities are unique per
		// plan, so there is no older signal to restore — cancellation removes
		// it via the returned Written undo key.
		return WriteResult{State: previous, Skipped: true}, nil
	}
	next := model.SignalState{
		VesselID: vessel.ID, CycleID: cycleID, Generation: generation,
		Aspect: aspect, UpdatedAt: p.now().UTC(),
	}
	if !p.store.CompareAndSet(vessel.ID, pointerIf(hadPrevious, previous), next) {
		current, ok := p.store.Get(vessel.ID)
		if ok && current.CycleID == cycleID && current.Generation == generation && current.Aspect == aspect {
			return WriteResult{State: current, Skipped: true}, nil
		}
		return WriteResult{}, ErrSignalConflict
	}
	if err := ctx.Err(); err != nil {
		return WriteResult{State: next, Previous: pointerIf(hadPrevious, previous)}, err
	}
	state := next
	if _, err := p.recorder.Record(ctx, chamberID, model.EventSignalChanged, cycleID, generation, state); err != nil {
		return WriteResult{State: state, Previous: pointerIf(hadPrevious, previous)}, err
	}
	return WriteResult{State: state, Previous: pointerIf(hadPrevious, previous)}, nil
}

// UndoVessel reverses one vessel publication with compare-and-set fencing and
// records a compensation marker stamped with the publishing generation.
func (p *Publisher) UndoVessel(ctx context.Context, chamberID string, cycleID uuid.UUID, generation uint64, undo Undo) error {
	if undo.Restore != nil {
		restored := *undo.Restore
		restored.UpdatedAt = p.now().UTC()
		p.store.CompareAndSet(undo.VesselID, &undo.Written, restored)
	} else {
		p.store.CompareAndRemove(undo.VesselID, undo.Written)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err := p.recorder.Record(ctx, chamberID, model.EventSignalChanged, cycleID, generation, map[string]any{
		"vessel_id": undo.VesselID, "compensated": true,
	})
	return err
}

func pointerIf(ok bool, state model.SignalState) *model.SignalState {
	if !ok {
		return nil
	}
	copyOfState := state
	return &copyOfState
}
