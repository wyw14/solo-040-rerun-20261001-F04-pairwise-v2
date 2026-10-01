package signal

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
)

type failingStore struct {
	journal.Store
	mu              sync.Mutex
	failSignalAfter int
	signalAppends   int
	failCompensated bool
}

func (s *failingStore) Append(ctx context.Context, event model.Event) (model.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if event.Type == model.EventSignalChanged {
		var payload map[string]any
		_ = event.Decode(&payload)
		if compensated, _ := payload["compensated"].(bool); compensated {
			if s.failCompensated {
				return model.Event{}, errors.New("journal unavailable")
			}
			return s.Store.Append(ctx, event)
		}
		s.signalAppends++
		if s.signalAppends > s.failSignalAfter {
			return model.Event{}, errors.New("journal unavailable")
		}
	}
	return s.Store.Append(ctx, event)
}

func newTestPublisher(t *testing.T, failAfter int) (*Publisher, *Store, *failingStore, time.Time) {
	t.Helper()
	base := journal.NewMemoryStore()
	failing := &failingStore{Store: base, failSignalAfter: failAfter}
	recorder := journal.NewRecorder(failing, func() time.Time { return fixedTime })
	store := NewStore()
	return NewPublisher(store, recorder, func() time.Time { return fixedTime }), store, failing, fixedTime
}

var fixedTime = time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)

func testPlan(vessels ...model.Vessel) model.ConvoyPlan {
	plan := model.ConvoyPlan{
		ID: uuid.New(), ChamberID: "chamber-1",
		Direction: model.DirectionUpstream, Vessels: vessels,
		CreatedAt: fixedTime,
	}
	return plan
}

func testCycle(plan model.ConvoyPlan) model.TransitCycle {
	cycle, err := model.NewTransitCycle(plan.ChamberID, plan.ID, plan.Direction, 1, fixedTime)
	if err != nil {
		panic(err)
	}
	return cycle
}

func mustVessel(name string) model.Vessel {
	vessel, err := model.NewVessel(name, "CS-"+name, 40)
	if err != nil {
		panic(err)
	}
	return vessel
}

// Repeated publish requests return the same result and never write twice.
func TestPublishIsIdempotent(t *testing.T) {
	publisher, store, failing, _ := newTestPublisher(t, 100)
	plan := testPlan(mustVessel("a"), mustVessel("b"), mustVessel("c"))
	cycle := testCycle(plan)

	first, record, err := publisher.PublishPlan(context.Background(), plan, cycle, model.SignalProceed)
	if err != nil {
		t.Fatalf("first publish: %v", err)
	}
	if record.Status != PublicationCompleted || len(first.Published) != 3 {
		t.Fatalf("unexpected first result: status=%s published=%d", record.Status, len(first.Published))
	}

	second, secondRecord, err := publisher.PublishPlan(context.Background(), plan, cycle, model.SignalProceed)
	if err != nil {
		t.Fatalf("repeated publish: %v", err)
	}
	if secondRecord.Status != PublicationCompleted {
		t.Fatalf("repeated publish status = %s", secondRecord.Status)
	}
	if len(second.Published) != len(first.Published) || second.CycleID != first.CycleID {
		t.Fatalf("repeated publish returned a different result: %+v vs %+v", second, first)
	}
	for index, id := range first.Published {
		if second.Published[index] != id {
			t.Fatalf("published vessel order/identity changed on repeat")
		}
	}
	if failing.signalAppends != 3 {
		t.Fatalf("expected 3 durable signal events, got %d", failing.signalAppends)
	}
	for _, vessel := range plan.Vessels {
		state, ok := store.Get(vessel.ID)
		if !ok || state.Aspect != model.SignalProceed || !state.IdentityMatches(cycle.ID, cycle.Generation) {
			t.Fatalf("vessel %s signal not stamped with proceed for the lock generation", vessel.Name)
		}
	}
}

// A journal failure mid-fleet rolls back every durable vessel and leaves no
// partial signals behind; the settled answer is returned unchanged on retry.
func TestPublishPartialFailureCompensates(t *testing.T) {
	publisher, store, failing, _ := newTestPublisher(t, 1)
	plan := testPlan(mustVessel("a"), mustVessel("b"), mustVessel("c"))
	cycle := testCycle(plan)

	publication, record, err := publisher.PublishPlan(context.Background(), plan, cycle, model.SignalProceed)
	if err == nil {
		t.Fatal("expected journal failure to surface")
	}
	if record.Status != PublicationCompensated {
		t.Fatalf("status = %s, want compensated", record.Status)
	}
	if len(publication.Published) != 1 {
		t.Fatalf("expected one durable vessel before failure, got %d", len(publication.Published))
	}
	for _, vessel := range plan.Vessels {
		if _, ok := store.Get(vessel.ID); ok {
			t.Fatalf("vessel %s signal survived compensation", vessel.Name)
		}
	}
	statuses := map[uuid.UUID]VesselStatus{}
	for _, progress := range record.Vessels {
		statuses[progress.VesselID] = progress.Status
	}
	if statuses[publication.Published[0]] != VesselCompensated {
		t.Fatalf("durable vessel should be compensated, got %s", statuses[publication.Published[0]])
	}

	eventsBefore := failing.signalAppends
	again, againRecord, againErr := publisher.PublishPlan(context.Background(), plan, cycle, model.SignalProceed)
	if againErr == nil || againErr.Error() != err.Error() {
		t.Fatalf("repeated request should return the same failure, got %v", againErr)
	}
	if len(again.Published) != 1 || againRecord.Status != PublicationCompensated {
		t.Fatalf("repeated request must return the same settled record")
	}
	if failing.signalAppends != eventsBefore {
		t.Fatalf("repeated request wrote new events: %d before, %d after", eventsBefore, failing.signalAppends)
	}
}

// When compensation itself cannot be journaled, the publication stays failed
// and requires operator attention rather than reporting a clean rollback.
func TestPublishCompensationFailureStaysFailed(t *testing.T) {
	base := journal.NewMemoryStore()
	failing := &failingStore{Store: base, failSignalAfter: 1, failCompensated: true}
	recorder := journal.NewRecorder(failing, func() time.Time { return fixedTime })
	store := NewStore()
	publisher := NewPublisher(store, recorder, func() time.Time { return fixedTime })
	plan := testPlan(mustVessel("a"), mustVessel("b"))
	cycle := testCycle(plan)

	_, record, err := publisher.PublishPlan(context.Background(), plan, cycle, model.SignalProceed)
	if err == nil {
		t.Fatal("expected failure")
	}
	if record.Status != PublicationFailed {
		t.Fatalf("status = %s, want failed", record.Status)
	}
	// A repeated request keeps reporting the same failed answer.
	_, again, _ := publisher.PublishPlan(context.Background(), plan, cycle, model.SignalProceed)
	if again.Status != PublicationFailed {
		t.Fatalf("repeated request status = %s, want failed", again.Status)
	}
}

// Cancellation only touches signals carrying its own lock generation.
func TestCompensatePlanRespectsGenerationFence(t *testing.T) {
	publisher, store, _, _ := newTestPublisher(t, 100)
	plan := testPlan(mustVessel("a"), mustVessel("b"))
	cycle := testCycle(plan)

	if _, _, err := publisher.PublishPlan(context.Background(), plan, cycle, model.SignalProceed); err != nil {
		t.Fatalf("publish: %v", err)
	}
	// A newer lock run republished vessel a (new generation).
	newCycle := cycle
	newCycle.Generation = 2
	newCycle.ID = uuid.New()
	store.Set(plan.Vessels[0].ID, newCycle.ID, 2, model.SignalProceed, fixedTime)

	compensated, err := publisher.CompensatePlan(context.Background(), plan, cycle.ID, cycle.Generation)
	if err != nil {
		t.Fatalf("compensate: %v", err)
	}
	if compensated != 1 {
		t.Fatalf("compensated = %d, want 1 (only same-generation signal)", compensated)
	}
	current, ok := store.Get(plan.Vessels[0].ID)
	if !ok || !current.IdentityMatches(newCycle.ID, 2) {
		t.Fatal("newer generation signal must not be rolled back")
	}
	if _, ok := store.Get(plan.Vessels[1].ID); ok {
		t.Fatal("same-generation signal should have been removed")
	}
	record, ok := publisher.Progress().Get(plan.ID)
	if !ok || record.Status != PublicationCompensated {
		t.Fatalf("record status = %s, want compensated", record.Status)
	}
}

// Begin dedupes concurrent requests onto the same authoritative record.
func TestRegistrySingleFlight(t *testing.T) {
	registry := NewRegistry()
	plan := testPlan(mustVessel("a"))
	record, err := registry.Begin(plan.ID, uuid.New(), 1, plan.ChamberID, model.SignalProceed, plan.Vessels, fixedTime)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	duplicate, err := registry.Begin(plan.ID, uuid.New(), 2, plan.ChamberID, model.SignalProceed, plan.Vessels, fixedTime)
	if !errors.Is(err, ErrPublicationRunning) || duplicate != record {
		t.Fatalf("concurrent begin must return the running record, got err=%v same=%v", err, duplicate == record)
	}
	registry.finish(record, PublicationCompleted, nil, fixedTime)
	settled, err := registry.Begin(plan.ID, record.CycleID, 1, plan.ChamberID, model.SignalProceed, plan.Vessels, fixedTime)
	if !errors.Is(err, ErrPublicationTerminal) || settled.Status != PublicationCompleted {
		t.Fatalf("terminal begin should return settled record, got err=%v status=%s", err, settled.Status)
	}
}
