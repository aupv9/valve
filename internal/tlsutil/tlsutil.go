// Package tlsutil builds mutual-TLS gRPC credentials shared by every binary.
package tlsutil

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// Config locates the PEM files. All three are required to enable mTLS.
type Config struct {
	Cert       string
	Key        string
	CA         string
	ServerName string
}

// RegisterFlags adds -tls-cert, -tls-key, -tls-ca and -tls-server-name.
func RegisterFlags(fs *flag.FlagSet) *Config {
	c := &Config{}
	fs.StringVar(&c.Cert, "tls-cert", "", "PEM certificate for mTLS (with -tls-key and -tls-ca)")
	fs.StringVar(&c.Key, "tls-key", "", "PEM private key for mTLS")
	fs.StringVar(&c.CA, "tls-ca", "", "PEM CA bundle that peers must chain to")
	fs.StringVar(&c.ServerName, "tls-server-name", "", "name to verify in server certificates (default: the dialed host)")
	return c
}

// Enabled reports whether mTLS is configured.
func (c *Config) Enabled() bool { return c.Cert != "" || c.Key != "" || c.CA != "" }

func (c *Config) check() error {
	if c.Cert == "" || c.Key == "" || c.CA == "" {
		return errors.New("mTLS needs all of -tls-cert, -tls-key and -tls-ca")
	}
	return nil
}

// ServerCredentials requires a client certificate signed by the CA; insecure if unconfigured.
func (c *Config) ServerCredentials() (credentials.TransportCredentials, error) {
	if !c.Enabled() {
		return insecure.NewCredentials(), nil
	}
	if err := c.check(); err != nil {
		return nil, err
	}
	pool, err := loadPool(c.CA)
	if err != nil {
		return nil, err
	}
	kp, err := newKeyPair(c.Cert, c.Key)
	if err != nil {
		return nil, err
	}
	return credentials.NewTLS(&tls.Config{
		MinVersion:     tls.VersionTLS13,
		ClientAuth:     tls.RequireAndVerifyClientCert,
		ClientCAs:      pool,
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) { return kp.get() },
	}), nil
}

// ClientCredentials presents the certificate and verifies the server; insecure if unconfigured.
func (c *Config) ClientCredentials() (credentials.TransportCredentials, error) {
	if !c.Enabled() {
		return insecure.NewCredentials(), nil
	}
	if err := c.check(); err != nil {
		return nil, err
	}
	pool, err := loadPool(c.CA)
	if err != nil {
		return nil, err
	}
	kp, err := newKeyPair(c.Cert, c.Key)
	if err != nil {
		return nil, err
	}
	return credentials.NewTLS(&tls.Config{
		MinVersion:           tls.VersionTLS13,
		RootCAs:              pool,
		ServerName:           c.ServerName,
		GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) { return kp.get() },
	}), nil
}

func loadPool(path string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("no certificates in CA file %s", path)
	}
	return pool, nil
}

type keyPair struct {
	cert, key string

	mu      sync.Mutex
	cur     *tls.Certificate
	mod     time.Time
	checked time.Time
}

const recheckEvery = 10 * time.Second

func newKeyPair(cert, key string) (*keyPair, error) {
	kp := &keyPair{cert: cert, key: key}
	if _, err := kp.get(); err != nil {
		return nil, err
	}
	return kp, nil
}

func (kp *keyPair) get() (*tls.Certificate, error) {
	kp.mu.Lock()
	defer kp.mu.Unlock()
	now := time.Now()
	if kp.cur != nil && now.Sub(kp.checked) < recheckEvery {
		return kp.cur, nil
	}
	kp.checked = now
	mod, err := latestMod(kp.cert, kp.key)
	if err != nil {
		if kp.cur != nil {
			return kp.cur, nil
		}
		return nil, err
	}
	if kp.cur != nil && !mod.After(kp.mod) {
		return kp.cur, nil
	}
	c, err := tls.LoadX509KeyPair(kp.cert, kp.key)
	if err != nil {
		if kp.cur != nil {
			return kp.cur, nil
		}
		return nil, fmt.Errorf("load key pair: %w", err)
	}
	kp.cur, kp.mod = &c, mod
	return kp.cur, nil
}

func latestMod(paths ...string) (time.Time, error) {
	var latest time.Time
	for _, p := range paths {
		st, err := os.Stat(p)
		if err != nil {
			return time.Time{}, err
		}
		if st.ModTime().After(latest) {
			latest = st.ModTime()
		}
	}
	return latest, nil
}
