package hatch

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"

	utls "github.com/refraction-networking/utls"
	"golang.org/x/net/proxy"
)

type contextProxyDialer interface {
	proxy.Dialer
	proxy.ContextDialer
}

func NewChromeWSDialer(proxyURL *url.URL) *chromeTLSDialer {
	roots, err := x509.SystemCertPool()
	if err != nil || roots == nil {
		roots = x509.NewCertPool()
	}
	dialer := &chromeTLSDialer{netDialer: &net.Dialer{Timeout: 20 * time.Second, KeepAlive: 30 * time.Second}, roots: roots}
	if proxyURL != nil {
		dialer.proxyURL = proxyURL
	}
	return dialer
}

type chromeTLSDialer struct {
	netDialer *net.Dialer
	proxyURL  *url.URL
	roots     *x509.CertPool
}

func (d *chromeTLSDialer) Dial(network, address string) (net.Conn, error) {
	return d.DialContext(context.Background(), network, address)
}

func (d *chromeTLSDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	raw, err := d.dialRaw(ctx, network, address)
	if err != nil {
		return nil, err
	}

	serverName, _, err := net.SplitHostPort(address)
	if err != nil {
		_ = raw.Close()
		return nil, fmt.Errorf("parse websocket server address: %w", err)
	}
	// The Chrome preset's ALPN extension overrides Config.NextProtos and offers
	// h2 first. The gateway then negotiates h2 and rejects the HTTP/1.1 upgrade
	// gorilla writes, so pin ALPN to http/1.1 as Chrome does for WebSockets.
	spec, err := utls.UTLSIdToSpec(utls.HelloChrome_Auto)
	if err != nil {
		_ = raw.Close()
		return nil, fmt.Errorf("Chrome TLS preset: %w", err)
	}
	for _, ext := range spec.Extensions {
		if alpn, ok := ext.(*utls.ALPNExtension); ok {
			alpn.AlpnProtocols = []string{"http/1.1"}
		}
	}
	tlsConn := utls.UClient(raw, &utls.Config{
		ServerName: serverName,
		RootCAs:    d.roots,
		NextProtos: []string{"http/1.1"},
	}, utls.HelloCustom)
	if err := tlsConn.ApplyPreset(&spec); err != nil {
		_ = raw.Close()
		return nil, fmt.Errorf("Chrome TLS preset: %w", err)
	}
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		_ = raw.Close()
		return nil, fmt.Errorf("Chrome TLS handshake: %w", err)
	}
	return tlsConn, nil
}

func (d *chromeTLSDialer) dialRaw(ctx context.Context, network, address string) (net.Conn, error) {
	if d.proxyURL == nil {
		return d.netDialer.DialContext(ctx, network, address)
	}
	switch d.proxyURL.Scheme {
	case "http", "https":
		return d.dialHTTPProxy(ctx, network, address)
	case "socks5", "socks5h":
		proxyDialer, err := proxy.FromURL(d.proxyURL, d.netDialer)
		if err != nil {
			return nil, fmt.Errorf("websocket proxy: %w", err)
		}
		contextDialer, ok := proxyDialer.(proxy.ContextDialer)
		if !ok {
			return nil, fmt.Errorf("websocket proxy %q does not support context dialing", d.proxyURL.Scheme)
		}
		return contextDialer.DialContext(ctx, network, address)
	default:
		return nil, fmt.Errorf("unsupported websocket proxy scheme %q", d.proxyURL.Scheme)
	}
}

func (d *chromeTLSDialer) dialHTTPProxy(ctx context.Context, network, address string) (net.Conn, error) {
	proxyAddress := d.proxyURL.Host
	if _, _, err := net.SplitHostPort(proxyAddress); err != nil {
		proxyAddress = net.JoinHostPort(d.proxyURL.Hostname(), "80")
		if d.proxyURL.Scheme == "https" {
			proxyAddress = net.JoinHostPort(d.proxyURL.Hostname(), "443")
		}
	}
	conn, err := d.netDialer.DialContext(ctx, network, proxyAddress)
	if err != nil {
		return nil, err
	}
	if d.proxyURL.Scheme == "https" {
		proxyConn := tls.Client(conn, &tls.Config{ServerName: d.proxyURL.Hostname(), MinVersion: tls.VersionTLS12})
		if err := proxyConn.HandshakeContext(ctx); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("proxy TLS handshake: %w", err)
		}
		conn = proxyConn
	}
	if deadline, ok := ctx.Deadline(); ok {
		if err := conn.SetDeadline(deadline); err != nil {
			_ = conn.Close()
			return nil, err
		}
	}

	connectReq := &http.Request{
		Method: http.MethodConnect,
		URL:    &url.URL{Opaque: address},
		Host:   address,
		Header: make(http.Header),
	}
	if d.proxyURL.User != nil {
		password, _ := d.proxyURL.User.Password()
		credentials := base64.StdEncoding.EncodeToString([]byte(d.proxyURL.User.Username() + ":" + password))
		connectReq.Header.Set("Proxy-Authorization", "Basic "+credentials)
	}
	if err := connectReq.Write(conn); err != nil {
		_ = conn.Close()
		return nil, err
	}
	reader := bufio.NewReader(conn)
	response, err := http.ReadResponse(reader, connectReq)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	if response.StatusCode != http.StatusOK {
		_ = response.Body.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("proxy CONNECT returned HTTP %d", response.StatusCode)
	}
	if err := conn.SetDeadline(time.Time{}); err != nil {
		_ = conn.Close()
		return nil, err
	}
	if reader.Buffered() > 0 {
		return bufferedReaderConn{Conn: conn, reader: reader}, nil
	}
	return conn, nil
}

type bufferedReaderConn struct {
	net.Conn
	reader io.Reader
}

func (c bufferedReaderConn) Read(p []byte) (int, error) { return c.reader.Read(p) }
