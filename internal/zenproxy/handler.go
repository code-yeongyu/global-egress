// Package zenproxy exposes OpenCode Zen's public free models without forwarding
// caller credentials, and retries a rejected request through a different egress.
package zenproxy

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

const (
	defaultAttempts  = 8
	defaultUpstream  = "https://opencode.ai/zen"
	defaultProxy     = "http://127.0.0.1:3128"
	gatewayUserAgent = "global-egress-zen-public/1.0"
	maxRequestBody   = 32 << 20
	upstreamHost     = "opencode.ai"
	// egressSlotHeader is set by the forward proxy on the response it relays,
	// naming the pool slot that carried the request.
	egressSlotHeader = "X-Egress-Slot"
	egressIPHeader   = "X-Egress-IP"
)

// ReportCall names an exit the upstream rejected for a destination, so the pool
// can cool that slot down for that target instead of handing it to the next
// caller.
type ReportCall struct {
	Slot   string
	Target string
	Reason string
}

// Options configures the public Zen gateway.
type Options struct {
	Upstream      string
	ForwardProxy  string
	ProxyPassword string
	Attempts      int
	Logger        *slog.Logger
	// ReportExit, when set, is called once per rejected exit. It must not block
	// the request path: failures are the reporter's problem, not the caller's.
	ReportExit func(ReportCall)
}

// Handler serves the OpenAI-compatible public-model surface.
type Handler struct {
	upstream         *url.URL
	attempts         int
	logger           *slog.Logger
	transportFactory transportFactory
	reportExit       func(ReportCall)
	requestSequence  atomic.Uint64
}

// New builds a public Zen gateway.
func New(options Options) (*Handler, error) {
	return newWithTransportFactory(options, nil)
}

func newWithTransportFactory(options Options, factory transportFactory) (*Handler, error) {
	if options.Upstream == "" {
		options.Upstream = defaultUpstream
	}
	if options.ForwardProxy == "" {
		options.ForwardProxy = defaultProxy
	}
	if options.Attempts <= 0 {
		options.Attempts = defaultAttempts
	}
	if options.Attempts > 32 {
		return nil, fmt.Errorf("zenproxy: attempts %d exceeds maximum 32", options.Attempts)
	}
	if options.Logger == nil {
		options.Logger = slog.Default()
	}

	upstream, err := url.Parse(options.Upstream)
	if err != nil {
		return nil, fmt.Errorf("zenproxy: parse upstream: %w", err)
	}
	forwardProxy, err := url.Parse(options.ForwardProxy)
	if err != nil {
		return nil, fmt.Errorf("zenproxy: parse forward proxy: %w", err)
	}
	if upstream.Scheme != "https" || upstream.Host == "" {
		return nil, errors.New("zenproxy: upstream must be an https URL")
	}
	if forwardProxy.Scheme != "http" || forwardProxy.Host == "" {
		return nil, errors.New("zenproxy: forward proxy must be an http URL")
	}
	if factory == nil {
		factory = proxyTransportFactory(forwardProxy, options.ProxyPassword)
	}
	return &Handler{
		upstream:         upstream,
		attempts:         options.Attempts,
		logger:           options.Logger,
		transportFactory: factory,
		reportExit:       options.ReportExit,
	}, nil
}

// ServeHTTP handles health, model discovery, and chat completions.
func (h *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	switch {
	case request.Method == http.MethodGet && request.URL.Path == "/healthz":
		writeJSON(writer, http.StatusOK, map[string]string{"status": "ok"})
	case request.Method == http.MethodGet && request.URL.Path == "/v1/models":
		writeJSON(writer, http.StatusOK, map[string]any{"object": "list", "data": freeModels()})
	case request.Method == http.MethodPost && request.URL.Path == "/v1/chat/completions":
		h.serveChat(writer, request)
	default:
		http.NotFound(writer, request)
	}
}

func (h *Handler) serveChat(writer http.ResponseWriter, request *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(writer, request.Body, maxRequestBody))
	if err != nil {
		http.Error(writer, "request body is invalid or too large", http.StatusBadRequest)
		return
	}
	var envelope struct {
		Model string `json:"model"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		http.Error(writer, "request body must be JSON", http.StatusBadRequest)
		return
	}
	if !isFreeModel(envelope.Model) {
		http.Error(writer, "model is not a public Zen free model", http.StatusBadRequest)
		return
	}

	policy := "any=1;uniq=zen-" +
		strconv.FormatInt(time.Now().UnixNano(), 36) + "-" +
		strconv.FormatUint(h.requestSequence.Add(1), 36)
	var lastErr error
	for attempt := 1; attempt <= h.attempts; attempt++ {
		response, transport, recorder, err := h.attempt(request, body, policy)
		if err != nil {
			lastErr = err
			closeIdleConnections(transport)
			if isNoCandidateConnectError(err) {
				policy = "any=1"
			}
			continue
		}
		if retryableStatus(response.StatusCode) && attempt < h.attempts {
			h.reportRejectedExit(response, recorder)
			if err := response.Body.Close(); err != nil {
				h.logger.Debug("close rejected Zen response", slog.String("error_type", fmt.Sprintf("%T", err)))
			}
			closeIdleConnections(transport)
			continue
		}
		writer.Header().Set("X-Zen-Egress-Attempts", strconv.Itoa(attempt))
		h.writeResponse(writer, response, transport)
		return
	}
	// The message matters more than the type here: without it an egress
	// failure is indistinguishable from a proxy, TLS, or upstream fault.
	h.logger.Warn("all Zen egress attempts failed",
		slog.String("error_type", fmt.Sprintf("%T", lastErr)),
		slog.String("error", errorText(lastErr)))
	http.Error(writer, "all Zen egress attempts failed", http.StatusBadGateway)
}

func (h *Handler) attempt(
	inbound *http.Request,
	body []byte,
	policy string,
) (*http.Response, http.RoundTripper, *slotRecorder, error) {
	target := *h.upstream
	target.Path = strings.TrimRight(h.upstream.Path, "/") + inbound.URL.Path
	target.RawQuery = inbound.URL.RawQuery
	outbound, err := http.NewRequestWithContext(
		inbound.Context(),
		inbound.Method,
		target.String(),
		bytes.NewReader(body),
	)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("zenproxy: build upstream request: %w", err)
	}
	copyHeaders(outbound.Header, inbound.Header)
	outbound.Header.Del("Authorization")
	outbound.Header.Del("Proxy-Authorization")
	outbound.Header.Set("User-Agent", gatewayUserAgent)
	outbound.Host = upstreamHost

	transport, recorder := h.transportFactory(policy)
	response, err := (&http.Client{Transport: transport}).Do(outbound)
	if err != nil {
		return nil, transport, recorder, fmt.Errorf("zenproxy: upstream request: %w", err)
	}
	return response, transport, recorder, nil
}

// reportRejectedExit tells the pool which slot the upstream just rejected. The
// slot id comes from the CONNECT handshake the dialer recorded, since the
// upstream response itself never carries it. Without a slot there is nothing to
// cool down, so the call is skipped rather than guessed.
func (h *Handler) reportRejectedExit(response *http.Response, recorder *slotRecorder) {
	if h.reportExit == nil || recorder == nil {
		return
	}
	slot := recorder.slotID()
	if slot == "" {
		return
	}
	h.reportExit(ReportCall{
		Slot:   slot,
		Target: upstreamHost,
		Reason: "zen status " + strconv.Itoa(response.StatusCode),
	})
}

// errorText renders an error for logs without tripping a nil dereference.
func errorText(err error) string {
	if err == nil {
		return "none"
	}
	return err.Error()
}

func isNoCandidateConnectError(err error) bool {
	var statusErr *connectStatusError
	return errors.As(err, &statusErr) && statusErr.statusCode == http.StatusConflict
}

func retryableStatus(status int) bool {
	switch status {
	case http.StatusForbidden, http.StatusTooManyRequests,
		http.StatusInternalServerError, http.StatusBadGateway,
		http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}
