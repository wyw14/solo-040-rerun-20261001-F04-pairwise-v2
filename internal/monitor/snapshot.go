package monitor

import (
	"time"

	"github.com/wyw14/cry-96/internal/admission"
	"github.com/wyw14/cry-96/internal/cycle"
	"github.com/wyw14/cry-96/internal/gate"
	"github.com/wyw14/cry-96/internal/model"
	"github.com/wyw14/cry-96/internal/safety"
	"github.com/wyw14/cry-96/internal/signal"
)

type ChamberView struct {
	ChamberID    string                                 `json:"chamber_id"`
	Cycle        *model.TransitCycle                    `json:"cycle,omitempty"`
	Gates        map[model.Direction]model.GatePosition `json:"gates"`
	Queue        []admission.QueueEntry                 `json:"queue"`
	Signals      []model.SignalState                    `json:"signals"`
	Publications []signal.PublicationRecord             `json:"publications"`
	Emergency    model.EmergencyRecord                  `json:"emergency"`
	UpdatedAt    time.Time                              `json:"updated_at"`
}

// Snapshot aggregates current responsibilities without taking ownership.
type Snapshot struct {
	cycles   *cycle.Coordinator
	gates    *gate.State
	queue    *admission.Service
	signals  *signal.Service
	progress *signal.Registry
	safety   *safety.Service
	now      func() time.Time
}

func NewSnapshot(cycles *cycle.Coordinator, gates *gate.State, queue *admission.Service, signals *signal.Service, progress *signal.Registry, safetyService *safety.Service, now func() time.Time) *Snapshot {
	if now == nil {
		now = time.Now
	}
	return &Snapshot{cycles: cycles, gates: gates, queue: queue, signals: signals, progress: progress, safety: safetyService, now: now}
}

func (s *Snapshot) Chamber(chamberID string) ChamberView {
	view := ChamberView{
		ChamberID: chamberID, Gates: s.gates.Snapshot(chamberID),
		Queue: s.queue.List(chamberID), Signals: s.signals.States(),
		Emergency: s.safety.Status(chamberID), UpdatedAt: s.now().UTC(),
	}
	if s.progress != nil {
		view.Publications = s.progress.ByChamber(chamberID)
	}
	if cycleValue, ok := s.cycles.Current(chamberID); ok {
		view.Cycle = &cycleValue
	}
	return view
}

func (s *Snapshot) Chambers(ids []string) []ChamberView {
	result := make([]ChamberView, 0, len(ids))
	for _, chamberID := range ids {
		result = append(result, s.Chamber(chamberID))
	}
	return result
}
