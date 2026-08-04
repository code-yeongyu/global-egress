package zenproxy

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHandler_retries_without_unique_batch_when_no_fresh_exit_exists(t *testing.T) {
	// Given: the pool rejects the unique batch because every measured exit IP
	// is stale, while an unconstrained attempt can open and measure a tunnel.
	var policies []string
	attempt := 0
	handler, err := newWithTransportFactory(
		Options{Attempts: 2},
		func(policy string) (http.RoundTripper, *slotRecorder) {
			policies = append(policies, policy)
			return roundTripFunc(func(*http.Request) (*http.Response, error) {
				attempt++
				if attempt == 1 {
					return nil, &connectStatusError{statusCode: http.StatusConflict}
				}
				return response(http.StatusOK, `{"choices":[{"message":{"content":"hi"}}]}`), nil
			}), recorderWithSlot(slotForTest)
		},
	)
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/chat/completions",
		bytes.NewReader([]byte(
			`{"model":"deepseek-v4-flash-free","messages":[{"role":"user","content":"hi"}]}`,
		)),
	)
	writer := httptest.NewRecorder()

	// When
	handler.ServeHTTP(writer, request)

	// Then
	if writer.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %s", writer.Code, writer.Body.String())
	}
	if len(policies) != 2 {
		t.Fatalf("policies = %#v, want two attempts", policies)
	}
	if !strings.Contains(policies[0], "uniq=zen-") {
		t.Fatalf("first policy = %q, want a unique batch", policies[0])
	}
	if policies[1] != "any=1" {
		t.Fatalf("second policy = %q, want any=1 bootstrap fallback", policies[1])
	}
}
