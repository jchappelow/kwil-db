package rpcserver

import (
	"context"
	"encoding/json"

	jsonrpc "github.com/kwilteam/kwil-db/core/rpc/json"
)

type MethodHandler func(ctx context.Context, s *Server) (argsPtr any, handler func() (any, *jsonrpc.Error))

type Svc interface {
	Handlers() map[jsonrpc.Method]MethodHandler
}

func (s *Server) RegisterSvc(svc Svc) {
	for method, handler := range svc.Handlers() {
		s.log.Infof("Registering method %q", method)
		s.RegisterMethodHandler(method, handler)
	}
}

func (s *Server) RegisterMethodHandler(method jsonrpc.Method, h MethodHandler) {
	s.methodHandlers[method] = h
}

// handleMethod unmarshals into the appropriate params struct, and dispatches to
// the TxSvc method handler.
func (s *Server) handleMethod(ctx context.Context, method jsonrpc.Method, params json.RawMessage) (any, *jsonrpc.Error) {
	maker, have := s.methodHandlers[method]
	if !have {
		return nil, jsonrpc.NewError(jsonrpc.ErrorUnknownMethod, "unknown method", nil)
	}

	argsPtr, handler := maker(ctx, s)

	err := json.Unmarshal(params, argsPtr)
	if err != nil {
		return nil, jsonrpc.NewError(jsonrpc.ErrorInvalidParams, err.Error(), nil)
	}

	return handler()
}
