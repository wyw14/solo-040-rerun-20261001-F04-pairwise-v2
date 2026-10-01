package signal

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
)

type fakeJournal struct {
	mu        sync.Mutex
	events    []model.Event
	failAt    uint64 // append that fails (1-based); 0 disables
	failError error
	blockAt   uint64
	block     chan struct{}
}

func (f *fakeJournal) Append(ctx context.Context, event model.Event) (model.Event, error) {
	f.mu.Lock()
	if f.block != nil && uint64(len(f.events))+1 == f.blockAt {
		block := f.block
		// One-shot gate: compensation markers must not block on the same slot.
		f.blockAt = 0
		f.mu.Unlock()
		select {
		case <-block:
		case <-ctx.Done():
			return model.Event{}, ctx.Err()
		}
		f.mu.Lock()
	}
	if f.failAt != 0 && uint64(len(f.events))+1 == f.failAt {
		f.failAt = 0
		err := f.failError
		f.mu.Unlock()
		return model.Event{}, err
	}
	event.Sequence = uint64(len(f.events) + 1)
	f.events = append(f.events, event)
	f.mu.Unlock()
	return event, nil
}

func (f *fakeJournal) Events(_ context.Context, chamberID string, after uint64) ([]model.Event, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var result []model.Event
	for _, event := range f.events {
		if event.ChamberID == chamberID && event.Sequence > after {
			result = append(result, event)
		}
	}
	return result, nil
}

func (f *fakeJournal) SaveSnapshot(context.Context, model.ChamberSnapshot) error { return nil }
func (f *fakeJournal) Snapshot(context.Context, string) (model.ChamberSnapshot, bool, error) {
	return model.ChamberSnapshot{}, false, nil
}
func (f *fakeJournal) Chambers(_ context.Context) ([]string, error) { return nil, nil }

func (f *fakeJournal) signalEvents() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	total := 0
	for _, event := range f.events {
		if event.Type == model.EventSignalChanged {
			total++
		}
	}
	return total
}

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time {
	c.t = c.t.Add(time.Second)
	return c.t
}

type preflightStub struct {
	mu        sync.Mutex
	cycle     model.TransitCycle
	cancelled map[uuid.UUID]bool
}

func (p *preflightStub) check(_ context.Context, planID uuid.UUID, _ string) (model.TransitCycle, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cancelled[planID] {
		return model.TransitCycle{}, errors.New("plan cancelled")
	}
	return p.cycle, nil
}

func newTestRig(t *testing.T) (*Manager, *Store, *fakeJournal, *fakeClock, model.TransitCycle, []model.Vessel) {
	t.Helper()
	clock := &fakeClock{t: time.Unix(1000, 0)}
	store := NewStore()
	jrnl := &fakeJournal{failError: errors.New("journal unavailable")}
	recorder := journal.NewRecorder(jrnl, clock.now)
	publisher := NewPublisher(store, recorder, clock.now)
	cycleID := uuid.New()
	cycle := model.TransitCycle{
		ID: cycleID, ChamberID: "chamber-1", PlanID: uuid.New(),
		Stage: model.StageAdmitting, Generation: 1,
	}
	stub := &preflightStub{cycle: cycle, cancelled: map[uuid.UUID]bool{}}
	manager := NewManager(context.Background(), publisher, stub.check, clock.now)
	vessels := []model.Vessel{}
	for i := 0; i < 3; i++ {
		vessel, err := model.NewVessel(fmt.Sprintf("Vessel %d", i), fmt.Sprintf("CS-%d", i), 50+float64(i))
		if err != nil {
			t.Fatal(err)
		}
		vessels = append(vessels, vessel)
	}
	return manager, store, jrnl, clock, cycle, vessels
}

func waitFor(t *testing.T, manager *Manager, planID uuid.UUID, status AggregateStatus) AggregateView {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		view, ok := manager.Status(planID)
		if ok && view.Status == status {
			return view
		}
		time.Sleep(5 * time.Millisecond)
	}
	view, _ := manager.Status(planID)
	t.Fatalf("aggregate for %s never reached %s (got %s)", planID, status, view.Status)
	return AggregateView{}
}

func TestDispatchConcurrentCoalescing(t *testing.T) {
	manager, store, jrnl, _, cycle, vessels := newTestRig(t)
	req := DispatchRequest{PlanID: cycle.PlanID, Key: "k1", ChamberID: cycle.ChamberID, Vessels: vessels, Aspect: model.SignalProceed}

	const callers = 8
	var wg sync.WaitGroup
	views := make([]AggregateView, callers)
	wg.Add(callers)
	for i := 0; i < callers; i++ {
		go func(index int) {
			defer wg.Done()
			view, _, err := manager.Dispatch(context.Background(), req)
			if err != nil {
				t.Errorf("dispatch %d: %v", index, err)
				return
			}
			views[index] = view
		}(i)
	}
	wg.Wait()

	view := waitFor(t, manager, cycle.PlanID, StatusSucceeded)
	if view.Published+view.Skipped != len(vessels) {
		t.Fatalf("expected all vessels published, got %+v", view.Vessels)
	}
	for i := 1; i < callers; i++ {
		if views[i].ID != views[0].ID {
			t.Fatalf("caller %d observed a different aggregate: %s vs %s", i, views[i].ID, views[0].ID)
		}
	}
	for _, vessel := range vessels {
		state, ok := store.Get(vessel.ID)
		if !ok || state.Generation != cycle.Generation || state.Aspect != model.SignalProceed {
			t.Fatalf("vessel %s not published for the right generation: %+v", vessel.ID, state)
		}
	}
	forward := 0
	for _, event := range jrnl.events {
		if event.Type == model.EventSignalChanged {
			forward++
		}
	}
	if forward != len(vessels) {
		t.Fatalf("expected %d forward events, got %d", len(vessels), forward)
	}

	// A sequential repeat with the same key replays without new writes.
	replay, replayed, err := manager.Dispatch(context.Background(), req)
	if err != nil || !replayed || replay.ID != view.ID {
		t.Fatalf("expected same aggregate replay, got %+v replayed=%v err=%v", replay, replayed, err)
	}
	if len(jrnl.events) != forward {
		t.Fatalf("replay appended %d new events", len(jrnl.events)-forward)
	}
}

func TestDispatchFailureRollsBackAndRetries(t *testing.T) {
	manager, store, jrnl, _, cycle, vessels := newTestRig(t)
	// The second forward append fails: vessel one must roll back too.
	jrnl.failAt = 2

	req := DispatchRequest{PlanID: cycle.PlanID, Key: "k1", ChamberID: cycle.ChamberID, Vessels: vessels, Aspect: model.SignalProceed}
	view, _, err := manager.Dispatch(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if view.Status == StatusRunning {
		view = waitFor(t, manager, cycle.PlanID, StatusFailed)
	}
	if view.Status != StatusFailed {
		t.Fatalf("expected failed aggregate, got %s", view.Status)
	}
	// Everything written before the failure must be rolled back.
	for _, vessel := range vessels {
		if _, ok := store.Get(vessel.ID); ok {
			t.Fatalf("vessel %s signal survived failure rollback", vessel.ID)
		}
	}

	// A retry with a new key resumes and publishes the whole convoy.
	retryReq := req
	retryReq.Key = "k2"
	retry, _, err := manager.Dispatch(context.Background(), retryReq)
	if err != nil {
		t.Fatal(err)
	}
	if retry.Status != StatusSucceeded {
		t.Fatalf("retry status = %s, want succeeded", retry.Status)
	}
	for _, vessel := range vessels {
		if _, ok := store.Get(vessel.ID); !ok {
			t.Fatalf("vessel %s missing after successful retry", vessel.ID)
		}
	}
}

func TestAbortAfterSuccessCancelsSignals(t *testing.T) {
	manager, store, _, _, cycle, vessels := newTestRig(t)
	req := DispatchRequest{PlanID: cycle.PlanID, Key: "k1", ChamberID: cycle.ChamberID, Vessels: vessels, Aspect: model.SignalProceed}
	view, _, err := manager.Dispatch(context.Background(), req)
	if err != nil || view.Status != StatusSucceeded {
		t.Fatalf("unexpected dispatch: %+v err=%v", view, err)
	}

	aborted, ok := manager.Abort(cycle.PlanID)
	if !ok || aborted.Status != StatusCancelled {
		t.Fatalf("abort view = %+v ok=%v", aborted, ok)
	}
	for _, vessel := range vessels {
		if _, exists := store.Get(vessel.ID); exists {
			t.Fatalf("vessel %s still signals proceed after abort", vessel.ID)
		}
	}
	if _, _, err := manager.Dispatch(context.Background(), req); !errors.Is(err, ErrPlanAborted) {
		t.Fatalf("expected ErrPlanAborted, got %v", err)
	}
}

func TestAbortDuringExecution(t *testing.T) {
	manager, store, jrnl, _, cycle, vessels := newTestRig(t)
	// Block the second vessel append so the dispatch is in flight.
	jrnl.blockAt = 2
	jrnl.block = make(chan struct{})

	req := DispatchRequest{PlanID: cycle.PlanID, Key: "k1", ChamberID: cycle.ChamberID, Vessels: vessels, Aspect: model.SignalProceed}
	go func() { _, _, _ = manager.Dispatch(context.Background(), req) }()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := store.Get(vessels[0].ID); ok {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if _, ok := store.Get(vessels[0].ID); !ok {
		t.Fatal("first vessel never became visible before abort")
	}

	blockedAppend := jrnl.block
	view, had := manager.Abort(cycle.PlanID)
	close(blockedAppend)
	if !had {
		t.Fatal("expected abort to find the aggregate")
	}
	waitFor(t, manager, cycle.PlanID, StatusCancelled)
	if view.Status == StatusRunning {
		// Abort joins, so the returned view should already be terminal.
		t.Fatalf("abort returned a running aggregate: %+v", view)
	}
	for _, vessel := range vessels {
		if _, exists := store.Get(vessel.ID); exists {
			t.Fatalf("vessel %s signal survived mid-flight abort", vessel.ID)
		}
	}
}

func TestExactKeyRejectsNewerGeneration(t *testing.T) {
	manager, _, _, _, cycle, vessels := newTestRig(t)
	req := DispatchRequest{PlanID: cycle.PlanID, Key: "k1", ChamberID: cycle.ChamberID, Vessels: vessels, Aspect: model.SignalProceed}
	if view, _, err := manager.Dispatch(context.Background(), req); err != nil || view.Status != StatusSucceeded {
		t.Fatalf("dispatch: view=%+v err=%v", view, err)
	}

	// Chamber cycled to a new generation.
	newCycle := cycle
	newCycle.ID = uuid.New()
	newCycle.Generation = cycle.Generation + 1
	manager.preflight = func(context.Context, uuid.UUID, string) (model.TransitCycle, error) {
		return newCycle, nil
	}
	_, _, err := manager.Dispatch(context.Background(), req)
	if !errors.Is(err, ErrIdentityConflict) {
		t.Fatalf("expected ErrIdentityConflict, got %v", err)
	}
}

func TestExternalPreReleasedSignalSkippedAndRemovedOnCancel(t *testing.T) {
	manager, store, jrnl, _, cycle, vessels := newTestRig(t)
	// Admission pre-releases the head vessel with the identical generation.
	store.Set(vessels[0].ID, cycle.ID, cycle.Generation, model.SignalProceed, time.Now())

	req := DispatchRequest{PlanID: cycle.PlanID, Key: "", ChamberID: cycle.ChamberID, Vessels: vessels, Aspect: model.SignalProceed}
	view, _, err := manager.Dispatch(context.Background(), req)
	if err != nil || view.Status != StatusSucceeded {
		t.Fatalf("dispatch: view=%+v err=%v", view, err)
	}
	if view.Skipped != 1 || view.Published != len(vessels)-1 {
		t.Fatalf("expected one skipped and %d published, got %+v", len(vessels)-1, view)
	}
	forward := 0
	for _, event := range jrnl.events {
		if event.Type == model.EventSignalChanged {
			forward++
		}
	}
	if forward != len(vessels)-1 {
		t.Fatalf("skipped external signal must not append a forward event, got %d", forward)
	}

	manager.Abort(cycle.PlanID)
	for _, vessel := range vessels {
		if _, ok := store.Get(vessel.ID); ok {
			t.Fatalf("cancelled plan must leave no proceed signal, vessel %s remains", vessel.ID)
		}
	}
}
