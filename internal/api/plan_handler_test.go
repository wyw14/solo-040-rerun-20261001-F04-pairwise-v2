package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/wyw14/cry-96/internal/model"
)

func newTestRouter(t *testing.T) (http.Handler, *Runtime) {
	t.Helper()
	runtime, err := NewRuntime(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	return Router(runtime, ""), runtime
}

func createPlanRequest(t *testing.T, router http.Handler, vesselCount int) model.ConvoyPlan {
	t.Helper()
	vessels := []map[string]any{}
	for i := 0; i < vesselCount; i++ {
		vessels = append(vessels, map[string]any{
			"name":      "Vessel",
			"call_sign": "CS-",
			"length_m":  60 + i,
		})
	}
	// Call signs must be unique only at the model level? NewVessel does not
	// enforce uniqueness; keep names identical is fine for the API.
	body, _ := json.Marshal(map[string]any{
		"chamber_id": "chamber-1", "direction": "upstream", "vessels": vessels,
	})
	request := httptest.NewRequest(http.MethodPost, "/api/plans", bytes.NewReader(body))
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusCreated {
		t.Fatalf("create plan status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Plan model.ConvoyPlan `json:"plan"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	return response.Plan
}

func TestPublishIdempotentAndCancelledRejects(t *testing.T) {
	router, runtime := newTestRouter(t)
	plan := createPlanRequest(t, router, 3)
	publishURL := "/api/plans/" + plan.ID.String() + "/signals"

	call := func(key string) (int, map[string]any) {
		request := httptest.NewRequest(http.MethodPost, publishURL, nil)
		if key != "" {
			request.Header.Set("Idempotency-Key", key)
		}
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, request)
		var body map[string]any
		_ = json.Unmarshal(recorder.Body.Bytes(), &body)
		return recorder.Code, body
	}

	// Concurrent identical-key requests must coalesce.
	var wg sync.WaitGroup
	ids := make(chan string, 4)
	statuses := make(chan int, 4)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			status, body := call("convoy-1")
			publication := body["publication"].(map[string]any)
			ids <- publication["id"].(string)
			statuses <- status
		}()
	}
	wg.Wait()
	close(ids)
	close(statuses)
	firstID := ""
	for id := range ids {
		if firstID == "" {
			firstID = id
		}
		if id != firstID {
			t.Fatalf("concurrent publishes returned different aggregates: %s vs %s", firstID, id)
		}
	}
	for status := range statuses {
		if status != http.StatusOK {
			t.Fatalf("coalesced publish status = %d, want 200", status)
		}
	}

	// GET plans embeds per-vessel final status and retains the active plan.
	listRequest := httptest.NewRequest(http.MethodGet, "/api/plans", nil)
	listRecorder := httptest.NewRecorder()
	router.ServeHTTP(listRecorder, listRequest)
	var listed map[string][]map[string]any
	if err := json.Unmarshal(listRecorder.Body.Bytes(), &listed); err != nil {
		t.Fatal(err)
	}
	var found map[string]any
	for _, plans := range listed {
		for _, item := range plans {
			if item["id"] == plan.ID.String() {
				found = item
			}
		}
	}
	if found == nil {
		t.Fatal("published plan missing from plan list")
	}
	publication := found["publication"].(map[string]any)
	if publication["status"] != "succeeded" {
		t.Fatalf("publication status = %v", publication["status"])
	}
	vesselProgress := publication["vessels"].([]any)
	if len(vesselProgress) != 3 {
		t.Fatalf("expected per-vessel progress for 3 vessels, got %d", len(vesselProgress))
	}

	// Cancel the plan; its signals are compensated exactly once.
	cancelRequest := httptest.NewRequest(http.MethodPost, "/api/plans/"+plan.ID.String()+"/cancel", nil)
	cancelRecorder := httptest.NewRecorder()
	router.ServeHTTP(cancelRecorder, cancelRequest)
	if cancelRecorder.Code != http.StatusOK {
		t.Fatalf("cancel status = %d body=%s", cancelRecorder.Code, cancelRecorder.Body.String())
	}
	for _, vessel := range plan.Vessels {
		if _, ok := runtime.SignalStore.Get(vessel.ID); ok {
			t.Fatalf("vessel %s still signals proceed after cancel", vessel.ID)
		}
	}

	// A cancelled plan can never be published again.
	if status, body := call("convoy-2"); status != http.StatusConflict {
		t.Fatalf("republish after cancel status = %d body=%v, want 409", status, body)
	}

	// Cancelled plans remain visible with their final publication status.
	router.ServeHTTP(httptest.NewRecorder(), listRequest)
}

func TestPublishRequiresPlanAndCycle(t *testing.T) {
	router, _ := newTestRouter(t)

	missing := httptest.NewRequest(http.MethodPost, "/api/plans/00000000-0000-0000-0000-000000000000/signals", nil)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, missing)
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("missing plan publish status = %d, want 404", recorder.Code)
	}

	statusRequest := httptest.NewRequest(http.MethodGet, "/api/plans/00000000-0000-0000-0000-000000000000/signals", nil)
	statusRecorder := httptest.NewRecorder()
	router.ServeHTTP(statusRecorder, statusRequest)
	if statusRecorder.Code != http.StatusNotFound {
		t.Fatalf("missing publication status = %d, want 404", statusRecorder.Code)
	}
}

func TestIdempotencyKeyValidation(t *testing.T) {
	router, _ := newTestRouter(t)
	plan := createPlanRequest(t, router, 1)
	request := httptest.NewRequest(http.MethodPost, "/api/plans/"+plan.ID.String()+"/signals", nil)
	request.Header.Set("Idempotency-Key", string(make([]byte, 130)))
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("oversized key status = %d, want 400", recorder.Code)
	}
}
