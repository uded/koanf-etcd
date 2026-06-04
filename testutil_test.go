package etcd

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/goleak"
)

// TestMain configures goleak to ignore well-known noisy goroutines from
// gRPC, etcd, and the standard library so per-test goleak.VerifyNone is
// meaningful.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		goleak.IgnoreTopFunction("google.golang.org/grpc.(*ccBalancerWrapper).watcher"),
		goleak.IgnoreTopFunction("google.golang.org/grpc/internal/transport.(*controlBuffer).get"),
		goleak.IgnoreTopFunction("google.golang.org/grpc/internal/transport.(*http2Client).keepalive"),
		goleak.IgnoreAnyFunction("google.golang.org/grpc.(*addrConn).resetTransport"),
		goleak.IgnoreTopFunction("go.etcd.io/etcd/client/v3.(*lessor).deadlineLoop"),
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
	)
}

// embeddedEtcd boots a single-node embedded etcd on random localhost ports
// and returns a connected clientv3.Client plus a teardown func. The
// teardown is registered with t.Cleanup so callers don't need to defer it.
func embeddedEtcd(t *testing.T) *clientv3.Client {
	t.Helper()

	dir, err := os.MkdirTemp("", "koanf-etcd-test-*")
	if err != nil {
		t.Fatalf("mkdir temp: %v", err)
	}

	cfg := embed.NewConfig()
	cfg.Dir = dir
	cfg.LogLevel = "error"
	cfg.ListenClientUrls = []url.URL{{Scheme: "http", Host: pickAddr(t)}}
	cfg.AdvertiseClientUrls = cfg.ListenClientUrls
	cfg.ListenPeerUrls = []url.URL{{Scheme: "http", Host: pickAddr(t)}}
	cfg.AdvertisePeerUrls = cfg.ListenPeerUrls
	cfg.InitialCluster = fmt.Sprintf("default=%s", cfg.ListenPeerUrls[0].String())

	e, err := embed.StartEtcd(cfg)
	if err != nil {
		os.RemoveAll(dir)
		t.Fatalf("start embedded etcd: %v", err)
	}

	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(15 * time.Second):
		e.Close()
		os.RemoveAll(dir)
		t.Fatalf("embedded etcd did not become ready in 15s")
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{cfg.ListenClientUrls[0].Host},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		e.Close()
		os.RemoveAll(dir)
		t.Fatalf("new client: %v", err)
	}

	t.Cleanup(func() {
		_ = cli.Close()
		e.Close()
		select {
		case <-e.Server.StopNotify():
		case <-time.After(5 * time.Second):
		}
		os.RemoveAll(dir)
	})

	return cli
}

// pickAddr finds a free localhost TCP port and returns "127.0.0.1:N".
func pickAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("pick free port: %v", err)
	}
	defer l.Close()
	return l.Addr().String()
}

// tlsFixture bundles paths to a self-signed CA plus server and client
// cert/key pairs generated on the fly. All files live in a temp dir
// cleaned up by t.Cleanup.
type tlsFixture struct {
	dir     string
	caFile  string
	srvCert string
	srvKey  string
	cliCert string
	cliKey  string
}

// genTLSFixtures generates a self-signed CA, server cert/key, and client
// cert/key, writing them to a temp dir. Returns the paths.
func genTLSFixtures(t *testing.T) tlsFixture {
	t.Helper()
	dir, err := os.MkdirTemp("", "koanf-etcd-tls-*")
	if err != nil {
		t.Fatalf("mkdir tls: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate ca key: %v", err)
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create ca cert: %v", err)
	}
	caFile := filepath.Join(dir, "ca.pem")
	writePEM(t, caFile, "CERTIFICATE", caDER)

	makeLeaf := func(cn string, isServer bool) (string, string) {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatalf("generate %s key: %v", cn, err)
		}
		tmpl := &x509.Certificate{
			SerialNumber: big.NewInt(time.Now().UnixNano()),
			Subject:      pkix.Name{CommonName: cn},
			NotBefore:    time.Now().Add(-time.Hour),
			NotAfter:     time.Now().Add(24 * time.Hour),
			KeyUsage:     x509.KeyUsageDigitalSignature,
		}
		if isServer {
			tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
			tmpl.DNSNames = []string{"localhost"}
			tmpl.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
		} else {
			tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
		}
		der, err := x509.CreateCertificate(rand.Reader, tmpl, caTmpl, &key.PublicKey, caKey)
		if err != nil {
			t.Fatalf("create %s cert: %v", cn, err)
		}
		certFile := filepath.Join(dir, cn+".pem")
		keyFile := filepath.Join(dir, cn+".key")
		writePEM(t, certFile, "CERTIFICATE", der)
		keyDER, err := x509.MarshalECPrivateKey(key)
		if err != nil {
			t.Fatalf("marshal %s key: %v", cn, err)
		}
		writePEM(t, keyFile, "EC PRIVATE KEY", keyDER)
		return certFile, keyFile
	}

	srvCert, srvKey := makeLeaf("server", true)
	cliCert, cliKey := makeLeaf("client", false)

	return tlsFixture{
		dir:     dir,
		caFile:  caFile,
		srvCert: srvCert,
		srvKey:  srvKey,
		cliCert: cliCert,
		cliKey:  cliKey,
	}
}

func writePEM(t *testing.T, path, typ string, der []byte) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create %s: %v", path, err)
	}
	defer f.Close()
	if err := pem.Encode(f, &pem.Block{Type: typ, Bytes: der}); err != nil {
		t.Fatalf("encode pem %s: %v", path, err)
	}
}

// ctxWithTimeout returns a context with the given timeout and registers
// cancel via t.Cleanup.
func ctxWithTimeout(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

// TestEmbeddedEtcd_Sanity asserts the harness can boot etcd, accept a
// put, and read it back. If this fails, nothing else can be trusted.
func TestEmbeddedEtcd_Sanity(t *testing.T) {
	cli := embeddedEtcd(t)
	ctx := ctxWithTimeout(t, 5*time.Second)

	if _, err := cli.Put(ctx, "/sanity", "ok"); err != nil {
		t.Fatalf("put: %v", err)
	}
	resp, err := cli.Get(ctx, "/sanity")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if len(resp.Kvs) != 1 || string(resp.Kvs[0].Value) != "ok" {
		t.Fatalf("unexpected response: %+v", resp)
	}
}
