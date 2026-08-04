package zenproxy

import (
	"bufio"
	"context"
	"encoding/base64"
	"net"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

// fakeProxy speaks a real CONNECT handshake so the dialer is exercised against
// the wire format the production proxy actually uses, not a stubbed response.
type fakeProxy struct {
	listener net.Listener
	slot     string
	ip       string
	status   int
	// gotAuth records the Proxy-Authorization the dialer sent.
	gotAuth  chan string
	gotHost  chan string
	echoBody string
}

func startFakeProxy(t *testing.T, slot, ip string, status int, echoBody string) *fakeProxy {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	p := &fakeProxy{
		listener: listener, slot: slot, ip: ip, status: status,
		gotAuth: make(chan string, 4), gotHost: make(chan string, 4), echoBody: echoBody,
	}
	go p.serve()
	t.Cleanup(func() { _ = listener.Close() })
	return p
}

func (p *fakeProxy) serve() {
	for {
		conn, err := p.listener.Accept()
		if err != nil {
			return
		}
		go func(c net.Conn) {
			defer func() { _ = c.Close() }()
			reader := bufio.NewReader(c)
			request, err := http.ReadRequest(reader)
			if err != nil {
				return
			}
			p.gotAuth <- request.Header.Get("Proxy-Authorization")
			p.gotHost <- request.Host

			if p.status != http.StatusOK {
				_, _ = c.Write([]byte("HTTP/1.1 403 Forbidden\r\n\r\n"))
				return
			}
			reply := "HTTP/1.1 200 Connection Established\r\n"
			if p.slot != "" {
				reply += egressSlotHeader + ": " + p.slot + "\r\n"
			}
			if p.ip != "" {
				reply += egressIPHeader + ": " + p.ip + "\r\n"
			}
			reply += "\r\n"
			if p.echoBody != "" {
				reply += p.echoBody
			}
			_, _ = c.Write([]byte(reply))
			// keep the tunnel open briefly so the caller can read
			time.Sleep(200 * time.Millisecond)
		}(conn)
	}
}

func (p *fakeProxy) url(t *testing.T) *url.URL {
	t.Helper()
	u, err := url.Parse("http://" + p.listener.Addr().String())
	if err != nil {
		t.Fatalf("parse proxy url: %v", err)
	}
	return u
}

func TestConnectDialer_records_the_slot_from_a_real_connect_handshake(t *testing.T) {
	// Given: a proxy that names its exit only on the CONNECT reply.
	proxy := startFakeProxy(t, "jp-tokyo-jp1159", "203.0.113.7", http.StatusOK, "")
	recorder := &slotRecorder{}
	dial := newConnectDialer(proxy.url(t), "any=1;uniq=zen-x", "secret",
		recorder, &net.Dialer{Timeout: 5 * time.Second})

	// When
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := dial(ctx, "tcp", "opencode.ai:443")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	// Then
	if got := recorder.slotID(); got != "jp-tokyo-jp1159" {
		t.Fatalf("slot = %q, want jp-tokyo-jp1159", got)
	}
	if got := recorder.exitIP(); got != "203.0.113.7" {
		t.Fatalf("exit ip = %q, want 203.0.113.7", got)
	}
	if host := <-proxy.gotHost; host != "opencode.ai:443" {
		t.Fatalf("CONNECT host = %q, want opencode.ai:443", host)
	}
	wantAuth := "Basic " + base64.StdEncoding.EncodeToString([]byte("any=1;uniq=zen-x:secret"))
	if auth := <-proxy.gotAuth; auth != wantAuth {
		t.Fatalf("Proxy-Authorization = %q, want %q", auth, wantAuth)
	}
}

func TestConnectDialer_reports_no_slot_when_the_proxy_sends_none(t *testing.T) {
	proxy := startFakeProxy(t, "", "", http.StatusOK, "")
	recorder := &slotRecorder{}
	dial := newConnectDialer(proxy.url(t), "any=1", "pw", recorder, &net.Dialer{Timeout: 5 * time.Second})

	conn, err := dial(context.Background(), "tcp", "opencode.ai:443")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if got := recorder.slotID(); got != "" {
		t.Fatalf("slot = %q, want empty", got)
	}
}

func TestConnectDialer_fails_when_the_proxy_rejects_connect(t *testing.T) {
	proxy := startFakeProxy(t, "", "", http.StatusForbidden, "")
	recorder := &slotRecorder{}
	dial := newConnectDialer(proxy.url(t), "any=1", "pw", recorder, &net.Dialer{Timeout: 5 * time.Second})

	_, err := dial(context.Background(), "tcp", "opencode.ai:443")
	if err == nil {
		t.Fatal("dial succeeded despite a rejected CONNECT")
	}
	if !strings.Contains(err.Error(), "403") {
		t.Fatalf("error = %v, want it to mention status 403", err)
	}
}

func TestConnectDialer_replays_bytes_buffered_with_the_connect_reply(t *testing.T) {
	// Given: the proxy writes tunnel bytes in the same flush as the reply.
	proxy := startFakeProxy(t, "slot-1", "", http.StatusOK, "HELLO-TUNNEL")
	recorder := &slotRecorder{}
	dial := newConnectDialer(proxy.url(t), "any=1", "pw", recorder, &net.Dialer{Timeout: 5 * time.Second})

	conn, err := dial(context.Background(), "tcp", "opencode.ai:443")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	// Then: those bytes must not be lost.
	buf := make([]byte, len("HELLO-TUNNEL"))
	if err := conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("read tunnel: %v", err)
	}
	if string(buf[:n]) != "HELLO-TUNNEL" {
		t.Fatalf("tunnel bytes = %q, want HELLO-TUNNEL", string(buf[:n]))
	}
}
