package schedule

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/model"
	"github.com/wyw14/cry-96/internal/signal"
)

type Cancellation struct {
	Plan         model.ConvoyPlan `json:"plan"`
	RemovedQueue int              `json:"removed_queue"`
	Compensated  int              `json:"compensated"`
}

// Cancel reverts queue and field-signal effects as one observable operation.
// The caller (schedule.Service) holds the per-plan lock shared with Publish,
// so a plan can never be published after it is cancelled.
func (p *Planner) Cancel(ctx context.Context, planID uuid.UUID, note string, publisher *signal.Publisher, cycleID uuid.UUID, generation uint64) (Cancellation, error) {
	plan, err := p.require(planID)
	if err != nil {
		return Cancellation{}, err
	}
	if plan.Cancelled {
		return Cancellation{Plan: plan}, nil
	}
	removed := p.admission.RemovePlan(plan.ChamberID, plan)
	compensated, err := publisher.CompensatePlan(context.WithoutCancel(ctx), plan, cycleID, generation)
	if err != nil {
		p.admission.Restore(plan.ChamberID, removed)
		return Cancellation{}, errors.Join(errors.New("signal compensation failed"), err)
	}
	for index := range removed {
		removed[index].Released = false
	}
	p.admission.Restore(plan.ChamberID, removed)
	plan.Cancelled = true
	plan.CancelNote = note
	p.replace(plan)
	_, recordErr := p.recorder.Record(context.WithoutCancel(ctx), plan.ChamberID, model.EventPlanCancelled, plan.ID, 0, map[string]any{
		"note": note, "restored_queue": len(removed), "compensated_signals": compensated,
	})
	return Cancellation{
		Plan: plan, RemovedQueue: len(removed), Compensated: compensated,
	}, recordErr
}
