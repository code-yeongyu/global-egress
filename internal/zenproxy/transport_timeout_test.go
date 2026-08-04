package zenproxy

import (
	"net/http"
	"net/url"
	"testing"
	"time"
)

func TestProxyTransportFactory_bounds_stalled_response_headers_per_attempt(t *testing.T) {
	forwardProxy, err := url.Parse("http://127.0.0.1:3128")
	if err != nil {
		t.Fatalf("parse forward proxy: %v", err)
	}
	transport, _ := proxyTransportFactory(forwardProxy, "test-password")("any=1")
	httpTransport, ok := transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport type = %T, want *http.Transport", transport)
	}

	const want = 12 * time.Second
	if httpTransport.ResponseHeaderTimeout != want {
		t.Fatalf("response header timeout = %s, want %s", httpTransport.ResponseHeaderTimeout, want)
	}
	if total := time.Duration(defaultAttempts) * want; total >= 2*time.Minute {
		t.Fatalf("worst-case header wait = %s, want below two minutes", total)
	}
}
