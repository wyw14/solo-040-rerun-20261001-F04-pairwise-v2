package schedule

import (
	"context"
	"testing"
	"time"

	"github.com/wyw14/cry-96/internal/admission"
	"github.com/wyw14/cry-96/internal/gate"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
	"github.com/wyw14/cry-96/internal/signal"
)

var scheduleNow = time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)

func newScheduleService(t *testing.T) (*Service, *signal.Store, model.ConvoyPlan, model.TransitCycle) {
	t.Helper()
	store := journal.NewMemoryStore()
	recorder := journal.NewRecorder(store, func() time.Time { return scheduleNow })
	signalStore := signal.NewStore()
	publisher := signal.NewPublisher(signalStore, recorder, func() time.Time { return scheduleNow })
	queue := admission.NewQueue()
	gates := gate.NewState("chamber-1")
	controller := admission.NewController(queue, gates, signalStore, func() time.Time { return scheduleNow })
	admissionService := admission.NewService(controller, recorder, func() time.Time { return scheduleNow })
	planner := NewPlanner(admissionService, recorder, func() time.Time { return scheduleNow })
	service := NewService(planner, publisher)

	vessel, err := model.NewVessel("River Star", "RS-1", 90)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := planner.Create(context.Background(), "chamber-1", model.DirectionUpstream, []model.Vessel{vessel})
	if err != nil {
		t.Fatal(err)
	}
	cycle, err := model.NewTransitCycle(plan.ChamberID, plan.ID, plan.Direction, 1, scheduleNow)
	if err != nil {
		t.Fatal(err)
	}
	return service, signalStore, plan, cycle
}

// Publishing twice returns the same vessels and status without re-writing.
func TestServicePublishIdempotent(t *testing.T) {
	service, signalStore, plan, cycle := newScheduleService(t)

	first, record, err := service.Publish(context.Background(), plan.ID, cycle)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	second, secondRecord, err := service.Publish(context.Background(), plan.ID, cycle)
	if err != nil {
		t.Fatalf("repeated publish: %v", err)
	}
	if len(second.Published) != len(first.Published) || secondRecord.Status != record.Status {
		t.Fatal("repeated publish did not return the same result")
	}
	state, ok := signalStore.Get(plan.Vessels[0].ID)
	if !ok || !state.IdentityMatches(cycle.ID, cycle.Generation) {
		t.Fatal("signal missing the lock run/generation stamp")
	}
}

// A cancelled plan keeps its compensated record and refuses to publish again.
func TestServiceCancelledPlanCannotPublish(t *testing.T) {
	service, signalStore, plan, cycle := newScheduleService(t)

	if _, _, err := service.Publish(context.Background(), plan.ID, cycle); err != nil {
		t.Fatalf("publish: %v", err)
	}
	cancellation, err := service.Cancel(context.Background(), plan.ID, "operator cancellation", cycle.ID, cycle.Generation)
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if cancellation.Compensated != 1 {
		t.Fatalf("compensated = %d, want 1", cancellation.Compensated)
	}
	if !cancellation.Plan.Cancelled {
		t.Fatal("plan should be marked cancelled")
	}
	if _, ok := signalStore.Get(plan.Vessels[0].ID); ok {
		t.Fatal("cancelled plan vessel signal should be gone")
	}
	if _, _, err := service.Publish(context.Background(), plan.ID, cycle); err == nil {
		t.Fatal("publish after cancel must fail")
	}
	record, ok := service.Publication(plan.ID)
	if !ok || record.Status != signal.PublicationCompensated {
		t.Fatalf("publication record status = %s, want compensated", record.Status)
	}
}

// Cancelling a plan that never published still succeeds (zero compensation).
func TestServiceCancelWithoutPublication(t *testing.T) {
	service, _, plan, cycle := newScheduleService(t)
	cancellation, err := service.Cancel(context.Background(), plan.ID, "operator cancellation", cycle.ID, cycle.Generation)
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if cancellation.Compensated != 0 || !cancellation.Plan.Cancelled {
		t.Fatal("unexpected cancellation result")
	}
}
