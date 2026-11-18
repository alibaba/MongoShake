package utils

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	der  []byte
}

func newTestCA(t *testing.T, commonName string, serial int64, parent *testCA) *testCA {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}

	template := &x509.Certificate{
		SerialNumber:          big.NewInt(serial),
		Subject:               pkix.Name{CommonName: commonName},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	parentCert, parentKey := template, key
	if parent != nil {
		parentCert, parentKey = parent.cert, parent.key
	}

	der, err := x509.CreateCertificate(rand.Reader, template, parentCert, &key.PublicKey, parentKey)
	if err != nil {
		t.Fatalf("create CA certificate: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse CA certificate: %v", err)
	}

	return &testCA{cert: cert, key: key, der: der}
}

func newTestLeaf(t *testing.T, dnsName string, serial int64, parent *testCA, notBefore, notAfter time.Time) (*x509.Certificate, []byte) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate leaf key: %v", err)
	}

	template := &x509.Certificate{
		SerialNumber:          big.NewInt(serial),
		Subject:               pkix.Name{CommonName: dnsName},
		DNSNames:              []string{dnsName},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}

	parentCert, parentKey := template, key
	if parent != nil {
		parentCert, parentKey = parent.cert, parent.key
	}

	der, err := x509.CreateCertificate(rand.Reader, template, parentCert, &key.PublicKey, parentKey)
	if err != nil {
		t.Fatalf("create leaf certificate: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse leaf certificate: %v", err)
	}

	return cert, der
}

func writeTestCerts(t *testing.T, dir, name string, ders ...[]byte) string {
	t.Helper()

	var buf bytes.Buffer
	for _, der := range ders {
		if err := pem.Encode(&buf, &pem.Block{Type: "CERTIFICATE", Bytes: der}); err != nil {
			t.Fatalf("encode certificate: %v", err)
		}
	}

	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, buf.Bytes(), 0600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}

	return path
}

func TestNewChainVerifyingTLSConfig(t *testing.T) {
	dir := t.TempDir()
	ca := newTestCA(t, "test-ca", 1, nil)
	otherCA := newTestCA(t, "other-ca", 2, nil)

	caFile := writeTestCerts(t, dir, "ca.pem", ca.der)

	now := time.Now()
	_, trustedDER := newTestLeaf(t, "db.example.com", 10, ca, now.Add(-time.Hour), now.Add(time.Hour))
	_, mismatchedHostDER := newTestLeaf(t, "other.example.com", 11, ca, now.Add(-time.Hour), now.Add(time.Hour))
	_, selfSignedDER := newTestLeaf(t, "db.example.com", 12, nil, now.Add(-time.Hour), now.Add(time.Hour))
	_, foreignDER := newTestLeaf(t, "db.example.com", 13, otherCA, now.Add(-time.Hour), now.Add(time.Hour))
	_, expiredDER := newTestLeaf(t, "db.example.com", 14, ca, now.Add(-2*time.Hour), now.Add(-time.Hour))

	cfg, err := NewChainVerifyingTLSConfig(caFile)
	if err != nil {
		t.Fatalf("build TLS config: %v", err)
	}
	if cfg.VerifyConnection == nil {
		t.Fatal("VerifyConnection callback is nil")
	}

	cases := []struct {
		name    string
		ders    [][]byte
		wantErr bool
	}{
		{name: "trusted chain", ders: [][]byte{trustedDER}},
		{name: "hostname mismatch is still accepted", ders: [][]byte{mismatchedHostDER}},
		{name: "self-signed certificate", ders: [][]byte{selfSignedDER}, wantErr: true},
		{name: "certificate from another CA", ders: [][]byte{foreignDER}, wantErr: true},
		{name: "expired certificate", ders: [][]byte{expiredDER}, wantErr: true},
		{name: "no certificate", wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			state := tls.ConnectionState{}
			for _, der := range tc.ders {
				cert, err := x509.ParseCertificate(der)
				if err != nil {
					t.Fatalf("parse certificate: %v", err)
				}
				state.PeerCertificates = append(state.PeerCertificates, cert)
			}

			err := cfg.VerifyConnection(state)
			if tc.wantErr && err == nil {
				t.Fatal("expected verification error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("expected verification success, got %v", err)
			}
		})
	}
}

func TestNewChainVerifyingTLSConfigLoadsWholeBundle(t *testing.T) {
	dir := t.TempDir()
	root := newTestCA(t, "root", 1, nil)
	intermediate := newTestCA(t, "intermediate", 2, root)
	leaf, _ := newTestLeaf(t, "db.example.com", 3, intermediate, time.Now().Add(-time.Hour), time.Now().Add(time.Hour))

	// Root first, intermediate second: loading only the first certificate would
	// leave the leaf unverifiable because the peer presents just the leaf.
	caFile := writeTestCerts(t, dir, "bundle.pem", root.der, intermediate.der)

	cfg, err := NewChainVerifyingTLSConfig(caFile)
	if err != nil {
		t.Fatalf("build TLS config: %v", err)
	}
	if got := len(cfg.RootCAs.Subjects()); got != 2 {
		t.Fatalf("expected 2 certificates in the pool, got %d", got)
	}

	state := tls.ConnectionState{PeerCertificates: []*x509.Certificate{leaf}}
	if err := cfg.VerifyConnection(state); err != nil {
		t.Fatalf("expected the leaf to verify against the bundle, got %v", err)
	}
}

func TestNewChainVerifyingTLSConfigRejectsEmptyBundle(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.pem")
	if err := os.WriteFile(path, []byte("not a certificate"), 0600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}

	if _, err := NewChainVerifyingTLSConfig(path); err == nil {
		t.Fatal("expected an error for a bundle without certificates")
	}
}
