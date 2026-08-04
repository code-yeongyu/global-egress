package zenproxy

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestHandler_retries_anonymous_free_request_when_exit_is_rate_limited(t *testing.T) {
	// Given
	var policies []string
	var authorizations []string
	var bodies []string
	attempt := 0
	handler, err := newWithTransportFactory(Options{Attempts: 3}, func(policy string) (http.RoundTripper, *slotRecorder) {
		rec := recorderWithSlot(slotForTest)
		policies = append(policies, policy)
		return roundTripFunc(func(request *http.Request) (*http.Response, error) {
			attempt++
			authorizations = append(authorizations, request.Header.Get("Authorization"))
			body, readErr := io.ReadAll(request.Body)
			if readErr != nil {
				t.Fatalf("read outbound body: %v", readErr)
			}
			bodies = append(bodies, string(body))
			if attempt == 1 {
				return response(http.StatusTooManyRequests, `{"error":{"type":"FreeUsageLimitError"}}`), nil
			}
			return response(http.StatusOK, `{"choices":[{"message":{"content":"OK"}}]}`), nil
		}), rec
	})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	requestBody := []byte(`{"model":"deepseek-v4-flash-free","messages":[{"role":"user","content":"hi"}]}`)
	request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(requestBody))
	request.Header.Set("Authorization", "Bearer workspace-key-must-not-leave")
	recorder := httptest.NewRecorder()

	// When
	handler.ServeHTTP(recorder, request)

	// Then
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	if attempt != 2 {
		t.Fatalf("attempts = %d, want 2", attempt)
	}
	for index, authorization := range authorizations {
		if authorization != "" {
			t.Fatalf("attempt %d forwarded Authorization %q", index+1, authorization)
		}
	}
	if len(bodies) != 2 || bodies[0] != string(requestBody) || bodies[1] != string(requestBody) {
		t.Fatalf("request body was not replayed exactly: %#v", bodies)
	}
	if len(policies) != 2 || policies[0] != policies[1] {
		t.Fatalf("retry attempts did not share one unique-IP batch: %#v", policies)
	}
	if !strings.HasPrefix(policies[0], "any=1;uniq=zen-") {
		t.Fatalf("policy = %q, want anonymous unique-IP rotation policy", policies[0])
	}
}

func TestHandler_replaces_blocked_client_user_agent(t *testing.T) {
	// Given
	var outboundUserAgent string
	handler, err := newWithTransportFactory(Options{Attempts: 1}, func(string) (http.RoundTripper, *slotRecorder) {
		rec := recorderWithSlot(slotForTest)
		return roundTripFunc(func(request *http.Request) (*http.Response, error) {
			outboundUserAgent = request.Header.Get("User-Agent")
			return response(http.StatusOK, `{"choices":[]}`), nil
		}), rec
	})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/chat/completions",
		strings.NewReader(`{"model":"deepseek-v4-flash-free","messages":[]}`),
	)
	request.Header.Set("User-Agent", "Python-urllib/3.13")
	recorder := httptest.NewRecorder()

	// When
	handler.ServeHTTP(recorder, request)

	// Then
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if outboundUserAgent != "global-egress-zen-public/1.0" {
		t.Fatalf("outbound User-Agent = %q", outboundUserAgent)
	}
}

func response(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

func TestHandler_reports_exhausted_exit_to_pool_on_rate_limit(t *testing.T) {
	// Given: the first exit is rate limited, the second serves the request.
	var reported []ReportCall
	attempt := 0
	handler, err := newWithTransportFactory(Options{
		Attempts: 3,
		ReportExit: func(call ReportCall) {
			reported = append(reported, call)
		},
	}, func(policy string) (http.RoundTripper, *slotRecorder) {
		rec := recorderWithSlot(slotForTest)
		return roundTripFunc(func(request *http.Request) (*http.Response, error) {
			attempt++
			if attempt == 1 {
				res := response(http.StatusTooManyRequests, `{"error":{"type":"FreeUsageLimitError"}}`)
				res.Header.Set(egressSlotHeader, "jp-tokyo-jp1159")
				return res, nil
			}
			res := response(http.StatusOK, `{"choices":[{"message":{"content":"OK"}}]}`)
			res.Header.Set(egressSlotHeader, "kr-seoul-kr104")
			return res, nil
		}), rec
	})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	requestBody := []byte(`{"model":"mimo-v2.5-free","messages":[{"role":"user","content":"hi"}]}`)
	request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(requestBody))
	recorder := httptest.NewRecorder()

	// When
	handler.ServeHTTP(recorder, request)

	// Then: exactly the rate-limited slot is reported, and the serving one is not.
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	if len(reported) != 1 {
		t.Fatalf("reported %d exits, want exactly 1: %#v", len(reported), reported)
	}
	if reported[0].Slot != "jp-tokyo-jp1159" {
		t.Fatalf("reported slot = %q, want the rate-limited exit", reported[0].Slot)
	}
	if reported[0].Target != upstreamHost {
		t.Fatalf("reported target = %q, want %q", reported[0].Target, upstreamHost)
	}
}

func TestHandler_serves_request_when_reporting_the_exhausted_exit_fails(t *testing.T) {
	// Given: reporting panics/fails, the request must still be served.
	attempt := 0
	handler, err := newWithTransportFactory(Options{
		Attempts: 3,
		ReportExit: func(ReportCall) {
			// A report backend that is down must not break the request path.
		},
	}, func(policy string) (http.RoundTripper, *slotRecorder) {
		rec := recorderWithSlot(slotForTest)
		return roundTripFunc(func(request *http.Request) (*http.Response, error) {
			attempt++
			if attempt == 1 {
				res := response(http.StatusTooManyRequests, `{"error":{"type":"FreeUsageLimitError"}}`)
				res.Header.Set(egressSlotHeader, "jp-tokyo-jp1159")
				return res, nil
			}
			return response(http.StatusOK, `{"choices":[{"message":{"content":"OK"}}]}`), nil
		}), rec
	})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		bytes.NewReader([]byte(`{"model":"mimo-v2.5-free","messages":[]}`)))
	recorder := httptest.NewRecorder()

	// When
	handler.ServeHTTP(recorder, request)

	// Then
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
}

func TestHandler_does_not_report_when_the_proxy_named_no_exit(t *testing.T) {
	// Given: a rejection arrives without the proxy's slot header, so there is no
	// slot to cool down. Reporting a placeholder would blackhole a real slot id.
	var reported []ReportCall
	attempt := 0
	handler, err := newWithTransportFactory(Options{
		Attempts:   3,
		ReportExit: func(call ReportCall) { reported = append(reported, call) },
	}, func(policy string) (http.RoundTripper, *slotRecorder) {
		rec := recorderWithSlot("")
		return roundTripFunc(func(request *http.Request) (*http.Response, error) {
			attempt++
			if attempt == 1 {
				// No X-Egress-Slot header on this rejection.
				return response(http.StatusTooManyRequests, `{"error":{"type":"FreeUsageLimitError"}}`), nil
			}
			return response(http.StatusOK, `{"choices":[{"message":{"content":"OK"}}]}`), nil
		}), rec
	})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		bytes.NewReader([]byte(`{"model":"mimo-v2.5-free","messages":[]}`)))
	recorder := httptest.NewRecorder()

	// When
	handler.ServeHTTP(recorder, request)

	// Then
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if len(reported) != 0 {
		t.Fatalf("reported %d exits without a slot id, want 0: %#v", len(reported), reported)
	}
}

// slotForTest is the exit id the fake CONNECT handshake reports in handler
// tests, mirroring what the production dialer records.
const slotForTest = "jp-tokyo-jp1159"

func recorderWithSlot(slot string) *slotRecorder {
	rec := &slotRecorder{}
	rec.set(slot, "203.0.113.9")
	return rec
}
