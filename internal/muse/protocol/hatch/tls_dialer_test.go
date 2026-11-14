package hatch

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"strconv"
	"testing"
	"time"

	utls "github.com/refraction-networking/utls"
)

func TestChromeWSDialerUsesChromeHelloAndVerifiesCertificate(t *testing.T) {
	rootKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	rootTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "fixture root"},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	rootDER, err := x509.CreateCertificate(rand.Reader, rootTemplate, rootTemplate, &rootKey.PublicKey, rootKey)
	if err != nil {
		t.Fatal(err)
	}
	root, err := x509.ParseCertificate(rootDER)
	if err != nil {
		t.Fatal(err)
	}
	serverKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	leafTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "localhost"},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore:    now.Add(-time.Minute),
		NotAfter:     now.Add(time.Hour),
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTemplate, root, &serverKey.PublicKey, rootKey)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(leafDER)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(root)
	observedHello := make(chan *tls.ClientHelloInfo, 1)
	listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{leafDER}, PrivateKey: serverKey, Leaf: leaf}},
		MinVersion:   tls.VersionTLS12,
		// The production gateway prefers h2; the WebSocket dialer must still
		// land on http/1.1.
		NextProtos: []string{"h2", "http/1.1"},
		GetConfigForClient: func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
			observedHello <- hello
			return nil, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.(*tls.Conn).Handshake()
	}()

	chromeDialer := NewChromeWSDialer(nil)
	chromeDialer.roots = roots
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	address := net.JoinHostPort("localhost", strconv.Itoa(listener.Addr().(*net.TCPAddr).Port))
	conn, err := chromeDialer.DialContext(ctx, "tcp", address)
	if err != nil {
		t.Fatalf("Chrome TLS dial: %v", err)
	}
	defer conn.Close()
	state := conn.(*utls.UConn).ConnectionState()
	if len(state.VerifiedChains) == 0 || state.VerifiedChains[0][0].Subject.CommonName != "localhost" || !state.VerifiedChains[0][len(state.VerifiedChains[0])-1].Equal(root) {
		t.Fatal("server certificate chain was not verified by the configured root")
	}
	if state.NegotiatedProtocol != "http/1.1" {
		t.Fatalf("negotiated ALPN = %q, want http/1.1", state.NegotiatedProtocol)
	}
	select {
	case hello := <-observedHello:
		if hello.ServerName != "localhost" {
			t.Fatalf("SNI = %q", hello.ServerName)
		}
		if len(hello.SupportedProtos) != 1 || hello.SupportedProtos[0] != "http/1.1" {
			t.Fatalf("offered ALPN = %v, want [http/1.1]", hello.SupportedProtos)
		}
		tls13Offered := false
		for _, version := range hello.SupportedVersions {
			if version == utls.VersionTLS13 {
				tls13Offered = true
				break
			}
		}
		if !tls13Offered {
			t.Fatalf("supported versions = %v; Chrome ClientHello did not offer TLS 1.3", hello.SupportedVersions)
		}
	case <-ctx.Done():
		t.Fatal("server did not observe ClientHello")
	}
}

func TestChromeWSDialerRejectsUntrustedCertificate(t *testing.T) {
	listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{testSelfSignedCertificate(t)}})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			_ = conn.(*tls.Conn).Handshake()
			_ = conn.Close()
		}
	}()
	proxyDialer := NewChromeWSDialer(nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	address := net.JoinHostPort("localhost", strconv.Itoa(listener.Addr().(*net.TCPAddr).Port))
	conn, err := proxyDialer.DialContext(ctx, "tcp", address)
	if err == nil {
		_ = conn.Close()
		t.Fatal("untrusted certificate was accepted")
	}
}

func testSelfSignedCertificate(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	certificate := &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject:      pkix.Name{CommonName: "localhost"},
		DNSNames:     []string{"localhost"},
		NotBefore:    now.Add(-time.Minute),
		NotAfter:     now.Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, certificate, certificate, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: parsed}
}
