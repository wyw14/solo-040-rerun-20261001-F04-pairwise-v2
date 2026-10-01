package api

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/model"
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

func plansHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		result := make(map[string]any, len(runtime.Chambers))
		for _, chamberID := range runtime.Chambers {
			result[chamberID] = runtime.Schedule.List(chamberID)
		}
		writeJSON(w, http.StatusOK, result)
	}
}

// currentCycle resolves the transit generation a plan's signals belong to.
func currentCycle(runtime *Runtime, plan model.ConvoyPlan) (model.TransitCycle, bool) {
	cycleValue, ok := runtime.Cycles.Current(plan.ChamberID)
	if !ok || cycleValue.PlanID != plan.ID {
		return model.TransitCycle{}, false
	}
	return cycleValue, true
}

func publishSignalsHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		planID, err := uuid.Parse(chi.URLParam(r, "planID"))
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		plan, ok := runtime.Planner.Get(planID)
		if !ok {
			writeError(w, http.StatusNotFound, errors.New("plan not found"))
			return
		}
		if plan.Cancelled {
			writeError(w, http.StatusConflict, errors.New("cancelled convoy plan cannot be published"))
			return
		}
		cycleValue, ok := currentCycle(runtime, plan)
		if !ok {
			writeError(w, http.StatusConflict, errors.New("active transit not found"))
			return
		}
		publication, record, err := runtime.Schedule.Publish(r.Context(), planID, cycleValue)
		if err != nil {
			// A settled record (e.g. compensated) is still the authoritative
			// answer: return it so operators see per-vessel outcomes.
			if record.PlanID != uuid.Nil {
				writeJSON(w, http.StatusConflict, map[string]any{
					"error": err.Error(), "publication": record,
				})
				return
			}
			writeError(w, http.StatusConflict, err)
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]any{
			"publication": publication, "progress": record,
		})
	}
}

func publicationStatusHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		planID, err := uuid.Parse(chi.URLParam(r, "planID"))
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		record, ok := runtime.Schedule.Publication(planID)
		if !ok {
			writeError(w, http.StatusNotFound, errors.New("publication not found"))
			return
		}
		writeJSON(w, http.StatusOK, record)
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
			writeError(w, http.StatusNotFound, errors.New("plan not found"))
			return
		}
		cycleValue, hasCycle := currentCycle(runtime, plan)
		result, err := runtime.Schedule.Cancel(r.Context(), planID, "operator cancellation", cycleValue.ID, cycleValue.Generation)
		if err != nil {
			writeError(w, http.StatusConflict, err)
			return
		}
		if hasCycle {
			_, _ = runtime.Cycles.Cancel(r.Context(), plan.ChamberID, "operator cancellation")
		}
		writeJSON(w, http.StatusOK, result)
	}
}
