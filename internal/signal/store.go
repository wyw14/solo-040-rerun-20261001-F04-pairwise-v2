package signal

import (
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/model"
)

// Store keeps the latest field aspect for each vessel.
type Store struct {
	mu     sync.RWMutex
	states map[uuid.UUID]model.SignalState
}

func NewStore() *Store { return &Store{states: make(map[uuid.UUID]model.SignalState)} }

func (s *Store) Set(vesselID, cycleID uuid.UUID, generation uint64, aspect model.SignalAspect, now time.Time) model.SignalState {
	state := model.SignalState{
		VesselID: vesselID, CycleID: cycleID, Generation: generation,
		Aspect: aspect, UpdatedAt: now.UTC(),
	}
	s.mu.Lock()
	s.states[vesselID] = state
	s.mu.Unlock()
	return state
}

// CompareAndSet writes next only when the stored state still matches expected.
// A nil expected means the vessel must have no stored state. Identity covers
// cycle and generation so a newer transit can never be overwritten.
func (s *Store) CompareAndSet(vesselID uuid.UUID, expected *model.SignalState, next model.SignalState) bool {
	next.VesselID = vesselID
	next.UpdatedAt = next.UpdatedAt.UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.states[vesselID]
	if expected == nil {
		if ok {
			return false
		}
	} else if !ok || !sameSignalIdentity(current, *expected) {
		return false
	}
	s.states[vesselID] = next
	return true
}

// CompareAndRemove deletes the state only when it still matches expected.
func (s *Store) CompareAndRemove(vesselID uuid.UUID, expected model.SignalState) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.states[vesselID]
	if !ok || !sameSignalIdentity(current, expected) {
		return false
	}
	delete(s.states, vesselID)
	return true
}

func sameSignalIdentity(a, b model.SignalState) bool {
	return a.VesselID == b.VesselID && a.CycleID == b.CycleID &&
		a.Generation == b.Generation && a.Aspect == b.Aspect
}

func (s *Store) Get(vesselID uuid.UUID) (model.SignalState, bool) {
	s.mu.RLock()
	state, ok := s.states[vesselID]
	s.mu.RUnlock()
	return state, ok
}

func (s *Store) Remove(vesselID uuid.UUID) {
	s.mu.Lock()
	delete(s.states, vesselID)
	s.mu.Unlock()
}

func (s *Store) List() []model.SignalState {
	s.mu.RLock()
	result := make([]model.SignalState, 0, len(s.states))
	for _, state := range s.states {
		result = append(result, state)
	}
	s.mu.RUnlock()
	sort.Slice(result, func(i, j int) bool {
		return result[i].VesselID.String() < result[j].VesselID.String()
	})
	return result
}
