package schedule

import (
	"context"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/model"
	"github.com/wyw14/cry-96/internal/signal"
)

type Cancellation struct {
	Plan         model.ConvoyPlan      `json:"plan"`
	RemovedQueue int                   `json:"removed_queue"`
	Compensated  int                   `json:"compensated"`
	Publication  *signal.AggregateView `json:"publication,omitempty"`
	Warning      string                `json:"warning,omitempty"`
}

// Cancel marks the plan cancelled, aborts any in-flight publication through
// the signal manager (the sole compensator), then reverts queue membership.
// The signal manager is aborted before queue changes so no vessel write can
// land after the cancellation decision.
func (p *Planner) Cancel(ctx context.Context, planID uuid.UUID, note string, manager *signal.Manager) (Cancellation, error) {
	plan, err := p.require(planID)
	if err != nil {
		return Cancellation{}, err
	}
	if plan.Cancelled {
		result := Cancellation{Plan: plan}
		if view, ok := manager.Status(planID); ok {
			result.Compensated = view.Compensated
			result.Publication = &view
		}
		return result, nil
	}

	// Flip the plan first so every new dispatch fails preflight immediately.
	plan.Cancelled = true
	plan.CancelNote = note
	p.replace(plan)

	// Join the publication executor; its compensation is fenced by cycle and
	// generation and runs exactly once.
	view, hadPublication := manager.Abort(planID)

	removed := p.admission.RemovePlan(plan.ChamberID, plan)
	for index := range removed {
		removed[index].Released = false
	}
	p.admission.Restore(plan.ChamberID, removed)

	_, recordErr := p.recorder.Record(context.WithoutCancel(ctx), plan.ChamberID, model.EventPlanCancelled, plan.ID, 0, map[string]any{
		"note": note, "restored_queue": len(removed), "compensated_signals": view.Compensated,
	})

	result := Cancellation{
		Plan: plan, RemovedQueue: len(removed), Compensated: view.Compensated,
	}
	if hadPublication {
		publication := view
		result.Publication = &publication
		if view.Error != "" {
			result.Warning = "signal compensation incomplete: one or more vessel signals may remain"
		}
	}
	if result.Warning != "" {
		return result, cancellationError{message: result.Warning, recordErr: recordErr}
	}
	return result, recordErr
}

// cancellationError keeps the cancellation result observable while reporting
// that field signals may not have been fully reversed.
type cancellationError struct {
	message   string
	recordErr error
}

func (e cancellationError) Error() string {
	if e.recordErr != nil {
		return e.message + "; cancellation event recording failed: " + e.recordErr.Error()
	}
	return e.message
}

func (e cancellationError) Unwrap() error { return e.recordErr }
