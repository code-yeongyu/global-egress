package zenproxy

import (
	"bufio"
	"context"
	"encoding/base64"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"
)

type connectStatusError struct {
	statusCode int
}

func (e *connectStatusError) Error() string {
	return fmt.Sprintf("zenproxy: CONNECT rejected with status %d", e.statusCode)
}

// slotRecorder carries the egress slot the forward proxy reported during the
// CONNECT handshake. Go's http.Transport discards the CONNECT response, so the
// only way to learn which exit served a request is to perform the handshake
// ourselves and record it here.
type slotRecorder struct {
	mu   sync.Mutex
	slot string
	ip   string
}

func (r *slotRecorder) set(slot, ip string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.slot = slot
	r.ip = ip
}

// slot returns the exit recorded by the most recent handshake, or "" when the
// proxy named none.
func (r *slotRecorder) slotID() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.slot
}

func (r *slotRecorder) exitIP() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.ip
}

// newConnectDialer returns a DialContext that reaches addr through the forward
// proxy by issuing CONNECT itself, recording the egress headers the proxy
// answers with. The returned connection is the raw tunnel, so the caller's TLS
// handshake runs end to end with the real upstream.
func newConnectDialer(
	proxyURL *url.URL,
	policy string,
	password string,
	recorder *slotRecorder,
	dialer *net.Dialer,
) func(ctx context.Context, network, addr string) (net.Conn, error) {
	credentials := base64.StdEncoding.EncodeToString([]byte(policy + ":" + password))

	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := dialer.DialContext(ctx, "tcp", proxyURL.Host)
		if err != nil {
			return nil, fmt.Errorf("zenproxy: dial forward proxy: %w", err)
		}

		if deadline, ok := ctx.Deadline(); ok {
			if err := conn.SetDeadline(deadline); err != nil {
				closeConn(conn)
				return nil, fmt.Errorf("zenproxy: set CONNECT deadline: %w", err)
			}
		}

		request := &http.Request{
			Method: http.MethodConnect,
			URL:    &url.URL{Opaque: addr},
			Host:   addr,
			Header: http.Header{},
		}
		request.Header.Set("Proxy-Authorization", "Basic "+credentials)
		if err := request.Write(conn); err != nil {
			closeConn(conn)
			return nil, fmt.Errorf("zenproxy: write CONNECT: %w", err)
		}

		reader := bufio.NewReader(conn)
		response, err := http.ReadResponse(reader, request)
		if err != nil {
			closeConn(conn)
			return nil, fmt.Errorf("zenproxy: read CONNECT response: %w", err)
		}
		defer func() { _ = response.Body.Close() }()

		if response.StatusCode != http.StatusOK {
			closeConn(conn)
			return nil, &connectStatusError{statusCode: response.StatusCode}
		}

		// This is the whole point of dialing by hand: these headers exist only
		// on the CONNECT reply.
		recorder.set(response.Header.Get(egressSlotHeader), response.Header.Get(egressIPHeader))

		// Clear the handshake deadline; the request's own context governs from
		// here, and a leftover deadline would kill a long streaming response.
		if err := conn.SetDeadline(time.Time{}); err != nil {
			closeConn(conn)
			return nil, fmt.Errorf("zenproxy: clear CONNECT deadline: %w", err)
		}

		// The proxy may have buffered bytes past the CONNECT reply; hand those
		// to the caller instead of dropping them.
		if reader.Buffered() > 0 {
			peeked, err := reader.Peek(reader.Buffered())
			if err != nil {
				closeConn(conn)
				return nil, fmt.Errorf("zenproxy: drain CONNECT buffer: %w", err)
			}
			return &prefixedConn{Conn: conn, prefix: peeked}, nil
		}
		return conn, nil
	}
}

// prefixedConn replays bytes that arrived alongside the CONNECT reply before
// reading further from the socket.
type prefixedConn struct {
	net.Conn
	prefix []byte
}

func (c *prefixedConn) Read(p []byte) (int, error) {
	if len(c.prefix) > 0 {
		n := copy(p, c.prefix)
		c.prefix = c.prefix[n:]
		return n, nil
	}
	return c.Conn.Read(p)
}

func closeConn(conn net.Conn) {
	_ = conn.Close()
}
