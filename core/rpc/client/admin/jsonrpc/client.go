package adminclient

import (
	"context"

	"github.com/kwilteam/kwil-db/core/rpc/client/admin"
	userclient "github.com/kwilteam/kwil-db/core/rpc/client/user/jsonrpc"
	adminjson "github.com/kwilteam/kwil-db/core/rpc/json/admin"
	"github.com/kwilteam/kwil-db/core/types"
	adminTypes "github.com/kwilteam/kwil-db/core/types/admin"

	"github.com/kwilteam/kwil-db/core/rpc/client/user"
	jsonrpc "github.com/kwilteam/kwil-db/core/rpc/json"
	// "github.com/kwilteam/kwil-db/core/types"
	// "github.com/kwilteam/kwil-db/core/types/transactions"
)

type Client struct {
	*userclient.Client
}

func NewClient(url string, opts ...userclient.Opts) *Client {
	userClient := userclient.NewClient(url, opts...)
	return WrapUserClient(userClient)
}

func WrapUserClient(cl *userclient.Client) *Client {
	return &Client{
		Client: cl,
	}
}

var _ user.TxSvcClient = (*Client)(nil)  // via embedded userclient.Client
var _ admin.AdminClient = (*Client)(nil) // with extra methods

func (cl *Client) Approve(ctx context.Context, publicKey []byte) ([]byte, error) {
	cmd := &adminjson.ApproveRequest{
		PubKey: publicKey,
	}
	res := &jsonrpc.BroadcastResponse{}
	err := cl.CallMethod(ctx, string(adminjson.MethodValApprove), cmd, res)
	if err != nil {
		return nil, err
	}
	return res.TxHash, err
}

func (cl *Client) Join(ctx context.Context) ([]byte, error) {
	cmd := &adminjson.JoinRequest{}
	res := &jsonrpc.BroadcastResponse{}
	err := cl.CallMethod(ctx, string(adminjson.MethodValJoin), cmd, res)
	if err != nil {
		return nil, err
	}
	return res.TxHash, err
}

func (cl *Client) JoinStatus(ctx context.Context, pubkey []byte) (*types.JoinRequest, error) {
	cmd := &adminjson.JoinStatusRequest{
		PubKey: pubkey,
	}
	res := &adminjson.JoinStatusResponse{}
	err := cl.CallMethod(ctx, string(adminjson.MethodValJoinStatus), cmd, res)
	if err != nil {
		return nil, err
	}
	return res.JoinRequest, err
}

func (cl *Client) Leave(ctx context.Context) ([]byte, error) {
	cmd := &adminjson.LeaveRequest{}
	res := &jsonrpc.BroadcastResponse{}
	err := cl.CallMethod(ctx, string(adminjson.MethodValLeave), cmd, res)
	if err != nil {
		return nil, err
	}
	return res.TxHash, err
}

func (cl *Client) ListValidators(ctx context.Context) ([]*types.Validator, error) {
	cmd := &adminjson.ListValidatorsRequest{}
	res := &adminjson.ListValidatorsResponse{}
	err := cl.CallMethod(ctx, string(adminjson.MethodValList), cmd, res)
	if err != nil {
		return nil, err
	}
	return res.Validators, err
}

func (cl *Client) Peers(ctx context.Context) ([]*adminTypes.PeerInfo, error) {
	cmd := &adminjson.PeersRequest{}
	res := &adminjson.PeersResponse{}
	err := cl.CallMethod(ctx, string(adminjson.MethodPeers), cmd, res)
	if err != nil {
		return nil, err
	}
	return res.Peers, err
}

func (cl *Client) Remove(ctx context.Context, publicKey []byte) ([]byte, error) {
	cmd := &adminjson.RemoveRequest{
		PubKey: publicKey,
	}
	res := &jsonrpc.BroadcastResponse{}
	err := cl.CallMethod(ctx, string(adminjson.MethodValRemove), cmd, res)
	if err != nil {
		return nil, err
	}
	return res.TxHash, err
}

func (cl *Client) Status(ctx context.Context) (*adminTypes.Status, error) {
	cmd := &adminjson.StatusRequest{}
	res := &adminjson.StatusResponse{}
	err := cl.CallMethod(ctx, string(adminjson.MethodStatus), cmd, res)
	if err != nil {
		return nil, err
	}
	// TODO: convert!
	return nil, nil
}

func (cl *Client) Version(ctx context.Context) (string, error) {
	cmd := &jsonrpc.VersionRequest{}
	res := &jsonrpc.VersionResponse{}
	err := cl.CallMethod(ctx, string(adminjson.MethodVersion), cmd, res)
	if err != nil {
		return "", err
	}
	return res.KwilVersion, err
}

func (cl *Client) ListPendingJoins(ctx context.Context) ([]*types.JoinRequest, error) {
	cmd := &adminjson.ListJoinRequestsRequest{}
	res := &adminjson.ListJoinRequestsResponse{}
	err := cl.CallMethod(ctx, string(adminjson.MethodValListJoins), cmd, res)
	if err != nil {
		return nil, err
	}
	return res.JoinRequests, err
}

// GetConfig gets the current config from the node.
// It returns the config serialized as JSON.
func (cl *Client) GetConfig(ctx context.Context) ([]byte, error) {
	cmd := &adminjson.GetConfigRequest{}
	res := &adminjson.GetConfigResponse{}
	err := cl.CallMethod(ctx, string(adminjson.MethodConfig), cmd, res)
	if err != nil {
		return nil, err
	}
	return res.Config, err
}

func (cl *Client) Ping(ctx context.Context) (string, error) {
	cmd := &jsonrpc.PingRequest{
		Message: "ping",
	}
	res := &jsonrpc.PingResponse{}
	err := cl.CallMethod(ctx, string(jsonrpc.MethodPing), cmd, res)
	if err != nil {
		return "", err
	}
	return res.Message, nil
}
