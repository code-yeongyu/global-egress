package main

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestZenExitReporter_posts_the_slot_and_cooldown_to_the_control_api(t *testing.T) {
	// Given
	type captured struct {
		auth string
		body map[string]string
	}
	received := make(chan captured, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/report" {
			t.Errorf("path = %q, want /v1/report", r.URL.Path)
		}
		if r.Method != http.MethodPost {
			t.Errorf("method = %q, want POST", r.Method)
		}
		var body map[string]string
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode body: %v", err)
		}
		received <- captured{auth: r.Header.Get("Authorization"), body: body}
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte(`{"slot":"jp1159"}`)); err != nil {
			t.Errorf("write response: %v", err)
		}
	}))
	defer server.Close()

	reporter := newZenExitReporter(server.URL, "secret-token", 30*time.Minute, discardLogger())

	// When
	reporter.report("jp-tokyo-jp1159", "opencode.ai", "zen status 429")

	// Then
	select {
	case got := <-received:
		if got.auth != "Bearer secret-token" {
			t.Fatalf("Authorization = %q, want bearer token", got.auth)
		}
		if got.body["slot"] != "jp-tokyo-jp1159" {
			t.Fatalf("slot = %q", got.body["slot"])
		}
		if got.body["target"] != "opencode.ai" {
			t.Fatalf("target = %q", got.body["target"])
		}
		if got.body["cooldown"] != "30m0s" {
			t.Fatalf("cooldown = %q, want 30m0s", got.body["cooldown"])
		}
	case <-time.After(3 * time.Second):
		t.Fatal("control API never received the report")
	}
}

func TestZenExitReporter_survives_a_control_api_that_is_down(t *testing.T) {
	// Given: a control URL that refuses connections.
	reporter := newZenExitReporter("http://127.0.0.1:1", "token", time.Minute, discardLogger())

	// When / Then: reporting must return rather than panic or hang the caller.
	done := make(chan struct{})
	go func() {
		reporter.report("slot", "opencode.ai", "zen status 429")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("report blocked the caller when the control API was down")
	}
}

func TestZenExitReporter_survives_a_control_api_error_status(t *testing.T) {
	// Given
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()
	reporter := newZenExitReporter(server.URL, "token", time.Minute, discardLogger())

	// When / Then
	done := make(chan struct{})
	go func() {
		reporter.report("slot", "opencode.ai", "zen status 429")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("report blocked on an error status")
	}
}
