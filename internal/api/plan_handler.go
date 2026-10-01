package api

import (
	"errors"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/model"
	"github.com/wyw14/cry-96/internal/schedule"
	"github.com/wyw14/cry-96/internal/signal"
)

type vesselInput struct {
	Name     string  `json:"name"`
	CallSign string  `json:"call_sign"`
	LengthM  float64 `json:"length_m"`
}

type planInput struct {
	ChamberID string          `json:"chamber_id"`
	Direction model.Direction `json:"direction"`
	Vessels   []vesselInput   `json:"vessels"`
}

// planView embeds a plan with its latest publication aggregate.
type planView struct {
	model.ConvoyPlan
	Publication *signal.AggregateView `json:"publication,omitempty"`
}

func createPlanHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var input planInput
		if err := decodeJSON(r, &input); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		vessels := make([]model.Vessel, 0, len(input.Vessels))
		for _, item := range input.Vessels {
			vessel, err := model.NewVessel(item.Name, item.CallSign, item.LengthM)
			if err != nil {
				writeError(w, http.StatusBadRequest, err)
				return
			}
			vessels = append(vessels, vessel)
		}
		plan, err := runtime.Schedule.Create(r.Context(), input.ChamberID, input.Direction, vessels)
		if err != nil {
			writeError(w, http.StatusConflict, err)
			return
		}
		cycleValue, err := runtime.CycleService.Start(r.Context(), plan)
		if err != nil {
			writeError(w, http.StatusConflict, err)
			return
		}
		writeJSON(w, http.StatusCreated, map[string]any{"plan": plan, "cycle": cycleValue})
	}
}

// plansHandler returns every plan (including cancelled ones) with its latest
// publication aggregate so operators see per-vessel final status.
func plansHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		publications := runtime.Schedule.Publications()
		result := make(map[string][]planView, len(runtime.Chambers))
		for _, chamberID := range runtime.Chambers {
			plans := runtime.Planner.All(chamberID)
			views := make([]planView, 0, len(plans))
			for _, plan := range plans {
				views = append(views, planView{ConvoyPlan: plan, Publication: publicationPointer(publications, plan.ID)})
			}
			result[chamberID] = views
		}
		writeJSON(w, http.StatusOK, result)
	}
}

func publicationPointer(publications map[uuid.UUID]signal.AggregateView, planID uuid.UUID) *signal.AggregateView {
	if view, ok := publications[planID]; ok {
		return &view
	}
	return nil
}

func publishSignalsHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		planID, err := uuid.Parse(chi.URLParam(r, "planID"))
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		key, err := idempotencyKey(r)
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		publication, replayed, err := runtime.Schedule.Publish(r.Context(), planID, key)
		if err != nil {
			writeError(w, publishErrorStatus(err), err)
			return
		}
		status := http.StatusOK
		switch publication.Status {
		case signal.StatusRunning:
			status = http.StatusAccepted
		case signal.StatusFailed, signal.StatusCancelled:
			status = http.StatusConflict
		}
		writeJSON(w, status, map[string]any{"publication": publication, "replay": replayed})
	}
}

func publicationStatusHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		planID, err := uuid.Parse(chi.URLParam(r, "planID"))
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		publication, ok := runtime.Schedule.Publication(planID)
		if !ok {
			writeError(w, http.StatusNotFound, errors.New("publication not found"))
			return
		}
		writeJSON(w, http.StatusOK, publication)
	}
}

// idempotencyKey trims and bounds the optional Idempotency-Key header; it is
// scoped to the plan in the URL by the manager.
func idempotencyKey(r *http.Request) (string, error) {
	key := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if len(key) > 128 {
		return "", errors.New("Idempotency-Key must be at most 128 characters")
	}
	return key, nil
}

func publishErrorStatus(err error) int {
	switch {
	case errors.Is(err, schedule.ErrPlanNotFound):
		return http.StatusNotFound
	case errors.Is(err, signal.ErrPlanAborted),
		errors.Is(err, schedule.ErrPlanCancelled),
		errors.Is(err, schedule.ErrNoActiveTransit),
		errors.Is(err, schedule.ErrTransitFinished),
		errors.Is(err, schedule.ErrPlanMismatch),
		errors.Is(err, signal.ErrIdentityConflict):
		return http.StatusConflict
	default:
		return http.StatusInternalServerError
	}
}

func cancelPlanHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		planID, err := uuid.Parse(chi.URLParam(r, "planID"))
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		plan, ok := runtime.Planner.Get(planID)
		if !ok {
			writeError(w, http.StatusNotFound, schedule.ErrPlanNotFound)
			return
		}
		result, cancelErr := runtime.Schedule.Cancel(r.Context(), planID, "operator cancellation")
		_, _ = runtime.Cycles.Cancel(r.Context(), plan.ChamberID, "operator cancellation")
		if cancelErr != nil {
			// Compensation may be incomplete: keep the observable result but
			// report that field signals may remain for the plan.
			writeJSON(w, http.StatusInternalServerError, map[string]any{
				"error":  cancelErr.Error(),
				"result": result,
			})
			return
		}
		writeJSON(w, http.StatusOK, result)
	}
}
