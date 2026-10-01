package schedule

import (
	"context"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/model"
	"github.com/wyw14/cry-96/internal/signal"
)

// CycleLookup resolves the live transit for a chamber. cycle.Coordinator
// satisfies it without introducing an import of the cycle package.
type CycleLookup interface {
	Current(chamberID string) (model.TransitCycle, bool)
}

// Service exposes planning and signal-publication workflows to the API.
type Service struct {
	planner *Planner
	manager *signal.Manager
	cycles  CycleLookup
}

func NewService(baseCtx context.Context, planner *Planner, publisher *signal.Publisher, cycles CycleLookup) *Service {
	service := &Service{planner: planner, cycles: cycles}
	preflight := func(ctx context.Context, planID uuid.UUID, chamberID string) (model.TransitCycle, error) {
		plan, ok := planner.Get(planID)
		if !ok {
			return model.TransitCycle{}, ErrPlanNotFound
		}
		if plan.Cancelled {
			return model.TransitCycle{}, ErrPlanCancelled
		}
		cycleValue, ok := cycles.Current(chamberID)
		if !ok {
			return model.TransitCycle{}, ErrNoActiveTransit
		}
		if cycleValue.Stage.Terminal() {
			return model.TransitCycle{}, ErrTransitFinished
		}
		if plan.ChamberID != cycleValue.ChamberID || plan.ID != cycleValue.PlanID {
			return model.TransitCycle{}, ErrPlanMismatch
		}
		return cycleValue, nil
	}
	service.manager = signal.NewManager(baseCtx, publisher, preflight, nil)
	return service
}

func (s *Service) Create(ctx context.Context, chamberID string, direction model.Direction, vessels []model.Vessel) (model.ConvoyPlan, error) {
	return s.planner.Create(ctx, chamberID, direction, vessels)
}

// Publish deduplicates through the signal manager: concurrent and repeated
// requests for one plan coalesce onto a single publication.
func (s *Service) Publish(ctx context.Context, planID uuid.UUID, idempotencyKey string) (signal.AggregateView, bool, error) {
	plan, ok := s.planner.Get(planID)
	if !ok {
		return signal.AggregateView{}, false, ErrPlanNotFound
	}
	return s.manager.Dispatch(ctx, signal.DispatchRequest{
		PlanID: plan.ID, Key: idempotencyKey, ChamberID: plan.ChamberID,
		Vessels: plan.Vessels, Aspect: model.SignalProceed,
	})
}

func (s *Service) Cancel(ctx context.Context, planID uuid.UUID, note string) (Cancellation, error) {
	return s.planner.Cancel(ctx, planID, note, s.manager)
}

func (s *Service) Publication(planID uuid.UUID) (signal.AggregateView, bool) {
	return s.manager.Status(planID)
}

func (s *Service) Publications() map[uuid.UUID]signal.AggregateView {
	return s.manager.Latest()
}

func (s *Service) Active(chamberID string) []model.ConvoyPlan {
	return s.planner.Active(chamberID)
}
