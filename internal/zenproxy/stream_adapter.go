package zenproxy

import (
	"encoding/json"
	"fmt"
	"net/http"
)

func prepareUpstreamBody(body []byte) ([]byte, bool, error) {
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, false, fmt.Errorf("decode streaming request: %w", err)
	}
	stream, _ := payload["stream"].(bool)
	if !stream {
		return body, false, nil
	}
	payload["stream"] = false
	delete(payload, "stream_options")
	rewritten, err := json.Marshal(payload)
	if err != nil {
		return nil, false, fmt.Errorf("encode non-streaming request: %w", err)
	}
	return rewritten, true, nil
}

func writeEmulatedStream(writer http.ResponseWriter, response *http.Response) error {
	var completion struct {
		ID      string `json:"id"`
		Created int64  `json:"created"`
		Model   string `json:"model"`
		Choices []struct {
			Index        int            `json:"index"`
			FinishReason any            `json:"finish_reason"`
			Logprobs     any            `json:"logprobs"`
			Message      map[string]any `json:"message"`
		} `json:"choices"`
		Usage any `json:"usage"`
	}
	if err := json.NewDecoder(response.Body).Decode(&completion); err != nil {
		return fmt.Errorf("decode non-streaming completion: %w", err)
	}
	choices := make([]map[string]any, 0, len(completion.Choices))
	for _, choice := range completion.Choices {
		choices = append(choices, map[string]any{
			"index":         choice.Index,
			"delta":         choice.Message,
			"finish_reason": choice.FinishReason,
			"logprobs":      choice.Logprobs,
		})
	}
	chunk := map[string]any{
		"id":      completion.ID,
		"object":  "chat.completion.chunk",
		"created": completion.Created,
		"model":   completion.Model,
		"choices": choices,
		"usage":   completion.Usage,
	}
	encoded, err := json.Marshal(chunk)
	if err != nil {
		return fmt.Errorf("encode emulated stream: %w", err)
	}
	copyHeaders(writer.Header(), response.Header)
	writer.Header().Del("Content-Length")
	writer.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	writer.Header().Set("Cache-Control", "no-cache")
	writer.WriteHeader(response.StatusCode)
	if _, err := fmt.Fprintf(writer, "data: %s\n\ndata: [DONE]\n\n", encoded); err != nil {
		return fmt.Errorf("write emulated stream: %w", err)
	}
	return nil
}
