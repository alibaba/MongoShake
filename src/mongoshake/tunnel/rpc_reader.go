package tunnel

import (
	"net"
	"net/rpc"

	LOG "github.com/vinllen/log4go"
)

type RPCReader struct {
	server  *rpc.Server
	address string
}

var rpcReplayer []Replayer

func (tunnel *RPCReader) Link(replayers []Replayer) (err error) {
	rpcReplayer = replayers

	var listener net.Listener
	if listener, err = net.Listen("tcp", tunnel.address); err != nil {
		LOG.Critical("Rpc reader listen listenAddress [%s] failed", tunnel.address)
		return
	}

	tunnel.server = rpc.NewServer()
	tunnel.server.Register(new(TunnelRPC))

	go tunnel.server.Accept(listener)

	return nil
}

type TunnelRPC struct {
}

func (rpc *TunnelRPC) Transfer(message *TMessage, response *int64) error {
	// hash corresponding replayer and re-shard
	if message.Shard >= uint32(len(rpcReplayer)) {
		message.Shard %= uint32(len(rpcReplayer))
	}
	*response = rpcReplayer[message.Shard].Sync(message, nil)

	return nil
}
