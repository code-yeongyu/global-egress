package zenproxy

import (
	"bufio"
	"bytes"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

// realisticProxy answers CONNECT with the egress headers and then speaks plain
// HTTP on the tunnel, so the slot exists ONLY on the CONNECT reply -- exactly
// the production shape that a header-injecting stub cannot reproduce.
type realisticProxy struct {
	listener net.Listener
	mu       sync.Mutex
	calls    int
	slots    []string
}

func startRealisticProxy(t *testing.T, slots []string) *realisticProxy {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	p := &realisticProxy{listener: listener, slots: slots}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go p.handle(conn)
		}
	}()
	t.Cleanup(func() { _ = listener.Close() })
	return p
}

func (p *realisticProxy) handle(conn net.Conn) {
	defer func() { _ = conn.Close() }()
	reader := bufio.NewReader(conn)
	if _, err := http.ReadRequest(reader); err != nil {
		return
	}

	p.mu.Lock()
	index := p.calls
	p.calls++
	p.mu.Unlock()

	slot := "exhausted-exit"
	if index < len(p.slots) {
		slot = p.slots[index]
	}

	// CONNECT reply carries the slot. Nothing else ever will.
	_, _ = conn.Write([]byte(
		"HTTP/1.1 200 Connection Established\r\n" +
			egressSlotHeader + ": " + slot + "\r\n" +
			egressIPHeader + ": 203.0.113.5\r\n\r\n"))

	// Then behave as the upstream: 429 for the first exit, 200 afterwards.
	if _, err := http.ReadRequest(reader); err != nil {
		return
	}
	if index == 0 {
		_, _ = conn.Write([]byte("HTTP/1.1 429 Too Many Requests\r\nContent-Length: 2\r\n\r\n{}"))
		return
	}
	body := `{"choices":[{"message":{"content":"OK"}}]}`
	_, _ = conn.Write([]byte(
		"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: " +
			itoa(len(body)) + "\r\n\r\n" + body))
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var digits []byte
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	return string(digits)
}

// This is the test that fails if the slot is read from the upstream response
// instead of the CONNECT handshake.
func TestHandler_reports_the_exit_named_only_by_the_connect_handshake(t *testing.T) {
	// Given
	proxy := startRealisticProxy(t, []string{"jp-tokyo-jp1159", "nl-ams-nl22"})
	proxyURL, err := url.Parse("http://" + proxy.listener.Addr().String())
	if err != nil {
		t.Fatalf("parse proxy url: %v", err)
	}

	var reported []ReportCall
	var mu sync.Mutex
	handler, err := New(Options{
		ForwardProxy:  proxyURL.String(),
		ProxyPassword: "pw",
		Attempts:      3,
		ReportExit: func(call ReportCall) {
			mu.Lock()
			defer mu.Unlock()
			reported = append(reported, call)
		},
	})
	if err != nil {
		t.Fatalf("new handler: %v", err)
	}
	// Talk plain HTTP over the tunnel rather than TLS, so the test exercises
	// the CONNECT path without needing a certificate authority.
	handler.upstream = &url.URL{Scheme: "http", Host: upstreamHost}

	request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		bytes.NewReader([]byte(`{"model":"mimo-v2.5-free","messages":[]}`)))
	writer := httptest.NewRecorder()

	// When
	done := make(chan struct{})
	go func() { handler.ServeHTTP(writer, request); close(done) }()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("handler hung")
	}

	// Then
	if writer.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", writer.Code, writer.Body.String())
	}
	mu.Lock()
	defer mu.Unlock()
	if len(reported) != 1 {
		t.Fatalf("reported %d exits, want 1: %#v", len(reported), reported)
	}
	if reported[0].Slot != "jp-tokyo-jp1159" {
		t.Fatalf("reported slot = %q, want jp-tokyo-jp1159 (the exit that 429'd)", reported[0].Slot)
	}
	if reported[0].Target != upstreamHost {
		t.Fatalf("reported target = %q, want %q", reported[0].Target, upstreamHost)
	}
	if !strings.Contains(reported[0].Reason, "429") {
		t.Fatalf("reason = %q, want it to mention 429", reported[0].Reason)
	}
}
