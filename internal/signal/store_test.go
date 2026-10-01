package signal

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/model"
)

func testState(vesselID uuid.UUID, cycleID uuid.UUID, generation uint64, aspect model.SignalAspect) model.SignalState {
	return model.SignalState{VesselID: vesselID, CycleID: cycleID, Generation: generation, Aspect: aspect, UpdatedAt: time.Unix(1, 0)}
}

func TestCompareAndSet(t *testing.T) {
	store := NewStore()
	vesselID := uuid.New()
	cycleA := uuid.New()
	cycleB := uuid.New()

	first := testState(vesselID, cycleA, 1, model.SignalProceed)
	if !store.CompareAndSet(vesselID, nil, first) {
		t.Fatal("expected CAS on absent state to succeed")
	}
	if store.CompareAndSet(vesselID, nil, first) {
		t.Fatal("expected CAS on absent state to fail when present")
	}
	// A stale generation must never overwrite the current one.
	stale := testState(vesselID, uuid.New(), 0, model.SignalStop)
	stale.CycleID = uuid.New()
	stale.Generation = 0
	if store.CompareAndSet(vesselID, &stale, testState(vesselID, cycleB, 2, model.SignalProceed)) {
		t.Fatal("expected CAS with mismatched expected identity to fail")
	}
	next := testState(vesselID, cycleA, 1, model.SignalHold)
	if !store.CompareAndSet(vesselID, &first, next) {
		t.Fatal("expected matching CAS to advance the aspect")
	}
	got, ok := store.Get(vesselID)
	if !ok || got.Aspect != model.SignalHold {
		t.Fatalf("unexpected stored state: %+v ok=%v", got, ok)
	}
}

func TestCompareAndRemove(t *testing.T) {
	store := NewStore()
	vesselID := uuid.New()
	state := testState(vesselID, uuid.New(), 1, model.SignalProceed)
	store.Set(vesselID, state.CycleID, state.Generation, state.Aspect, state.UpdatedAt)

	newer := state
	newer.Generation = 2
	if store.CompareAndRemove(vesselID, newer) {
		t.Fatal("expected remove to be fenced by generation")
	}
	if _, ok := store.Get(vesselID); !ok {
		t.Fatal("vessel state must survive a fenced remove attempt")
	}
	if !store.CompareAndRemove(vesselID, state) {
		t.Fatal("expected matching remove to succeed")
	}
	if _, ok := store.Get(vesselID); ok {
		t.Fatal("expected vessel state to be removed")
	}
}
