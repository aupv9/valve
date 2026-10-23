package tlsutil

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

type ca struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pem  []byte
}

func newCA(t *testing.T) *ca {
	t.Helper()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "valve-test-ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, _ := x509.ParseCertificate(der)
	return &ca{cert: cert, key: key, pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

// issue writes a leaf certificate for name into dir and returns a Config.
func (c *ca) issue(t *testing.T, dir, name string) *Config {
	t.Helper()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: name},
		DNSNames:  []string{name},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, c.cert, &key.PublicKey, c.key)
	if err != nil {
		t.Fatal(err)
	}
	kder, _ := x509.MarshalECPrivateKey(key)
	cfg := &Config{
		Cert: filepath.Join(dir, name+".crt"), Key: filepath.Join(dir, name+".key"), CA: filepath.Join(dir, name+"-ca.crt"),
	}
	write := func(p string, b []byte) {
		if err := os.WriteFile(p, b, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write(cfg.Cert, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	write(cfg.Key, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kder}))
	write(cfg.CA, c.pem)
	return cfg
}

func serve(t *testing.T, cfg *Config) string {
	t.Helper()
	creds, err := cfg.ServerCredentials()
	if err != nil {
		t.Fatal(err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	g := grpc.NewServer(grpc.Creds(creds))
	healthpb.RegisterHealthServer(g, health.NewServer())
	go g.Serve(lis) //nolint:errcheck // ends with Stop
	t.Cleanup(g.Stop)
	return lis.Addr().String()
}

func call(t *testing.T, addr string, cfg *Config, insecureClient bool) error {
	t.Helper()
	var opt grpc.DialOption
	if insecureClient {
		opt = grpc.WithTransportCredentials(insecure.NewCredentials())
	} else {
		creds, err := cfg.ClientCredentials()
		if err != nil {
			t.Fatal(err)
		}
		opt = grpc.WithTransportCredentials(creds)
	}
	conn, err := grpc.NewClient(addr, opt)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	_, err = healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	return err
}

func TestMutualTLS(t *testing.T) {
	dir := t.TempDir()
	trusted := newCA(t)
	server := trusted.issue(t, dir, "controller")
	addr := serve(t, server)

	client := trusted.issue(t, dir, "aggregator")
	client.ServerName = "controller"
	if err := call(t, addr, client, false); err != nil {
		t.Fatalf("trusted client rejected: %v", err)
	}

	stranger := newCA(t).issue(t, t.TempDir(), "aggregator")
	stranger.CA = client.CA // trusts the server, but its own cert is foreign
	stranger.ServerName = "controller"
	if err := call(t, addr, stranger, false); err == nil {
		t.Fatal("client with a certificate from another CA was accepted")
	}

	if err := call(t, addr, nil, true); err == nil {
		t.Fatal("plaintext client was accepted")
	}
}

func TestPartialConfigRejected(t *testing.T) {
	if _, err := (&Config{Cert: "x"}).ServerCredentials(); err == nil {
		t.Fatal("partial mTLS config accepted")
	}
	if (&Config{}).Enabled() {
		t.Fatal("empty config reported as enabled")
	}
}
