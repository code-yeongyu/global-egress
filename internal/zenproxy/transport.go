package zenproxy

import (
	"net"
	"net/http"
	"net/url"
	"time"
)

// transportFactory builds the transport for one attempt together with the
// recorder that captures which exit the forward proxy assigned to it.
type transportFactory func(policy string) (http.RoundTripper, *slotRecorder)

func proxyTransportFactory(forwardProxy *url.URL, password string) transportFactory {
	return func(policy string) (http.RoundTripper, *slotRecorder) {
		recorder := &slotRecorder{}
		// CONNECT is issued by newConnectDialer rather than by Transport.Proxy,
		// because Transport discards the CONNECT response and with it the only
		// record of which exit served the request.
		dial := newConnectDialer(
			forwardProxy,
			policy,
			password,
			recorder,
			&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second},
		)
		return &http.Transport{
			DialContext:           dial,
			ForceAttemptHTTP2:     true,
			DisableKeepAlives:     true,
			TLSHandshakeTimeout:   15 * time.Second,
			ResponseHeaderTimeout: 2 * time.Minute,
			ExpectContinueTimeout: time.Second,
		}, recorder
	}
}

func closeIdleConnections(transport http.RoundTripper) {
	if closer, ok := transport.(interface{ CloseIdleConnections() }); ok {
		closer.CloseIdleConnections()
	}
}
