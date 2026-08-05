package zenproxy

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHandler_emulatesSSE_through_the_reliable_non_streaming_upstream(t *testing.T) {
	var upstreamBody map[string]any
	handler, err := newWithTransportFactory(
		Options{Attempts: 1},
		func(string) (http.RoundTripper, *slotRecorder) {
			return roundTripFunc(func(request *http.Request) (*http.Response, error) {
				if err := json.NewDecoder(request.Body).Decode(&upstreamBody); err != nil {
					t.Fatalf("decode upstream request: %v", err)
				}
				return response(
					http.StatusOK,
					`{"id":"chatcmpl-zen","object":"chat.completion","created":1785893803,"model":"deepseek-v4-flash-free","choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":"JOBDORI-SENPI-OK","reasoning_content":"brief reasoning"}}],"usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15}}`,
				), nil
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
			`{"model":"deepseek-v4-flash-free","messages":[{"role":"user","content":"hi"}],"stream":true,"stream_options":{"include_usage":true}}`,
		)),
	)
	writer := httptest.NewRecorder()

	handler.ServeHTTP(writer, request)

	if upstreamBody["stream"] != false {
		t.Fatalf("upstream stream = %#v, want false", upstreamBody["stream"])
	}
	if _, exists := upstreamBody["stream_options"]; exists {
		t.Fatalf("upstream stream_options = %#v, want absent", upstreamBody["stream_options"])
	}
	if writer.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %s", writer.Code, writer.Body.String())
	}
	if contentType := writer.Header().Get("Content-Type"); !strings.HasPrefix(contentType, "text/event-stream") {
		t.Fatalf("content type = %q, want text/event-stream", contentType)
	}
	body := writer.Body.String()
	for _, fragment := range []string{
		`"object":"chat.completion.chunk"`,
		`"content":"JOBDORI-SENPI-OK"`,
		`"reasoning_content":"brief reasoning"`,
		`"finish_reason":"stop"`,
		"data: [DONE]\n\n",
	} {
		if !strings.Contains(body, fragment) {
			t.Fatalf("response missing %q: %s", fragment, body)
		}
	}
}
