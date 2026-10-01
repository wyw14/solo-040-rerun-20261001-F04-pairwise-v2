package schedule

import (
	"context"
	"errors"
	"sync"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/model"
	"github.com/wyw14/cry-96/internal/signal"
)

// Service exposes planning and signal-publication workflows to the API.
type Service struct {
	planner   *Planner
	publisher *signal.Publisher

	// publishLocks serialises publish and cancel for each plan, removing the
	// window in which a plan could be cancelled after the active-plan check but
	// before its signals were written.
	locksMu      sync.RWMutex
	publishLocks map[uuid.UUID]*sync.Mutex
}

func NewService(planner *Planner, publisher *signal.Publisher) *Service {
	return &Service{planner: planner, publisher: publisher, publishLocks: make(map[uuid.UUID]*sync.Mutex)}
}

func (s *Service) lockFor(planID uuid.UUID) *sync.Mutex {
	s.locksMu.RLock()
	lock, ok := s.publishLocks[planID]
	s.locksMu.RUnlock()
	if ok {
		return lock
	}
	s.locksMu.Lock()
	defer s.locksMu.Unlock()
	if lock, ok = s.publishLocks[planID]; ok {
		return lock
	}
	lock = &sync.Mutex{}
	s.publishLocks[planID] = lock
	return lock
}

func (s *Service) Create(ctx context.Context, chamberID string, direction model.Direction, vessels []model.Vessel) (model.ConvoyPlan, error) {
	return s.planner.Create(ctx, chamberID, direction, vessels)
}

// Publish issues the convoy signals for a plan. Repeated requests for the same
// plan return the same publication result; a request arriving while the first
// one is in flight waits for it instead of writing vessels a second time.
func (s *Service) Publish(ctx context.Context, planID uuid.UUID, cycle model.TransitCycle) (signal.Publication, signal.PublicationRecord, error) {
	lock := s.lockFor(planID)
	lock.Lock()
	defer lock.Unlock()

	plan, ok := s.planner.Get(planID)
	if !ok {
		return signal.Publication{}, signal.PublicationRecord{}, errors.New("convoy plan not found")
	}
	if plan.Cancelled {
		return signal.Publication{}, signal.PublicationRecord{}, errors.New("cancelled convoy plan cannot be published")
	}
	if plan.ChamberID != cycle.ChamberID || plan.ID != cycle.PlanID {
		return signal.Publication{}, signal.PublicationRecord{}, errors.New("plan and transit do not match")
	}
	return s.publisher.PublishPlan(ctx, plan, cycle, model.SignalProceed)
}

// Cancel reverts queue and field-signal effects as one observable operation.
// Holding the same per-plan lock as Publish guarantees a cancelled plan can
// never have signals published afterwards.
func (s *Service) Cancel(ctx context.Context, planID uuid.UUID, note string, cycleID uuid.UUID, generation uint64) (Cancellation, error) {
	lock := s.lockFor(planID)
	lock.Lock()
	defer lock.Unlock()
	return s.planner.Cancel(ctx, planID, note, s.publisher, cycleID, generation)
}

func (s *Service) Publication(planID uuid.UUID) (signal.PublicationRecord, bool) {
	return s.publisher.Progress().Get(planID)
}

func (s *Service) Active(chamberID string) []model.ConvoyPlan {
	return s.planner.Active(chamberID)
}

// List returns all plans for a chamber including cancelled ones.
func (s *Service) List(chamberID string) []model.ConvoyPlan {
	return s.planner.List(chamberID)
}
