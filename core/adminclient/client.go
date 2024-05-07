// package adminclient provides a client for the Kwil admin service.
// The admin service is used to perform node administrative actions,
// such as submitting validator transactions, retrieving node status, etc.
package adminclient

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"os"

	"github.com/kwilteam/kwil-db/core/log"
	adminRpc "github.com/kwilteam/kwil-db/core/rpc/client/admin"
	adminjson "github.com/kwilteam/kwil-db/core/rpc/client/admin/jsonrpc"
	rpcclient "github.com/kwilteam/kwil-db/core/rpc/client/user/jsonrpc"
	"github.com/kwilteam/kwil-db/core/rpc/transport"
	"github.com/kwilteam/kwil-db/core/types/transactions"
	"github.com/kwilteam/kwil-db/core/utils/url"
)

// AdminClient is a client for the Kwil admin service.
// It inherits both the admin and tx services.
type AdminClient struct {
	adminRpc.AdminClient // transport for admin client. we can just expose this, since we don't need to wrap it with any logic
	txClient             // should be the subset of the interface of core/client/Client that we want to expose here.

	log log.Logger

	// tls cert files, if using grpc and not unix socket
	kwildCertFile  string
	clientKeyFile  string
	clientCertFile string
}

// txClient is the txsvc client interface.
// It allows us to selectively expose the txsvc client methods.
type txClient interface {
	// Ping pings the connected node.
	Ping(ctx context.Context) (string, error)
	// TxQuery queries a transaction by hash.
	TxQuery(ctx context.Context, txHash []byte) (*transactions.TcTxQueryResponse, error)
}

// NewClient creates a new admin client . The target arg should be either
// "127.0.0.1:8485" or "/path/to/socket.socket", not a URL.
func NewClient(ctx context.Context, target string, opts ...AdminClientOpt) (*AdminClient, error) {
	c := &AdminClient{
		log: log.NewNoOp(),
	}

	// Subtle way to default to either http or https
	parsedTarget, err := url.ParseURLWithSchemeFallback(target, "http://")
	if err != nil {
		return nil, err
	}

	trans := http.DefaultTransport.(*http.Transport) // http.RoundTripper

	// we can have:
	// tcp + tls
	// tcp + no tls
	// unix socket + no tls

	// Set RootCAs if we have a kwild cert file.
	tlsConfig := transport.DefaultClientTLSConfig()
	if c.kwildCertFile != "" {
		pemCerts, err := os.ReadFile(c.kwildCertFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read cert file: %w", err)
		}
		if !tlsConfig.RootCAs.AppendCertsFromPEM(pemCerts) {
			return nil, errors.New("credentials: failed to append certificates")
		}
	}

	// Set Certificates for client authentication, if required
	if c.clientKeyFile != "" && c.clientCertFile != "" {
		authCert, err := tls.LoadX509KeyPair(c.clientCertFile, c.clientKeyFile)
		if err != nil {
			return nil, err
		}
		tlsConfig.Certificates = append(tlsConfig.Certificates, authCert)
	}

	// This TLS config does not mean it will use TLS. The scheme dictates that.
	trans.TLSClientConfig = tlsConfig

	switch parsedTarget.Scheme {
	case url.UNIX, url.HTTPS:
		// Consider these the "safe" schema to be used without TLS.
	default:
		return nil, fmt.Errorf("unknown scheme %q", parsedTarget.Scheme)
	}

	var clOpts []rpcclient.Opts
	rpcclient.Opts

	opts = append(opts, rpcclient.WithHTTPClient(&http.Client{
		Transport: trans,
	}))

	cl := adminjson.NewClient(parsedTargets, opts...)
	c.AdminClient = cl

	c.txClient = cl

	return c, nil
}

// newAuthenticatedTLSConfig creates a new tls.Config for an
// mutually-authenticated TLS (mTLS) client. In addition to the server's
// certificate file, the client's own key pair is required to support protocol
// level client authentication.
func newAuthenticatedTLSConfig(kwildCertFile, clientCertFile, clientKeyFile string) (*tls.Config, error) {
	cfg, err := transport.NewClientTLSConfigFromFile(kwildCertFile)
	if err != nil {
		return nil, err
	}

	authCert, err := tls.LoadX509KeyPair(clientCertFile, clientKeyFile)
	if err != nil {
		return nil, err
	}
	cfg.Certificates = append(cfg.Certificates, authCert)

	return cfg, nil
}
