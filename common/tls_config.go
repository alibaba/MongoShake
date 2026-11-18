package utils

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
)

// NewChainVerifyingTLSConfig loads the CA bundle in caFile and returns a client
// TLS config that verifies the peer certificate chain against it. Hostname
// verification stays disabled because endpoints may be addressed by IP while
// their certificates only carry DNS SANs; setting InsecureSkipVerify alone
// would disable the chain check as well. A certificate issued by the same CA
// for another hostname is therefore still accepted, so the CA should only sign
// the endpoints that MongoShake connects to.
func NewChainVerifyingTLSConfig(caFile string) (*tls.Config, error) {
	data, err := ioutil.ReadFile(caFile)
	if err != nil {
		return nil, err
	}

	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(data) {
		return nil, fmt.Errorf("no certificate found in %s", caFile)
	}

	return &tls.Config{
		RootCAs:            roots,
		InsecureSkipVerify: true,
		VerifyConnection: func(cs tls.ConnectionState) error {
			return verifyPeerChain(cs, roots)
		},
	}, nil
}

// verifyPeerChain checks the chain only; DNSName is deliberately left empty so
// hostname mismatches keep working.
func verifyPeerChain(cs tls.ConnectionState, roots *x509.CertPool) error {
	if len(cs.PeerCertificates) == 0 {
		return fmt.Errorf("server presented no certificate")
	}

	intermediates := x509.NewCertPool()
	for _, cert := range cs.PeerCertificates[1:] {
		intermediates.AddCert(cert)
	}

	_, err := cs.PeerCertificates[0].Verify(x509.VerifyOptions{
		Roots:         roots,
		Intermediates: intermediates,
		KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	})
	if err != nil {
		return fmt.Errorf("server certificate chain verification failed: %v", err)
	}

	return nil
}
