package tunnel

import (
	"net"
	"net/rpc"

	"mongoshake/common"

	LOG "github.com/vinllen/log4go"
)

type RPCWriter struct {
	RemoteAddr string

	// for golang rpc
	tcpAddr   *net.TCPAddr
	rpcConn   *net.TCPConn
	rpcClient *rpc.Client
}

func (tunnel *RPCWriter) Send(message *WMessage) int64 {
	var err error
	if tunnel.rpcConn == nil {
		// we try just one time as higher layer will handle this error
		if tunnel.rpcConn, err = net.DialTCP("tcp", nil, tunnel.tcpAddr); err != nil {
			LOG.Critical("Remote rpc server connect failed. %v", err)
			utils.YieldInMs(3000)
			tunnel.rpcConn = nil
			return ReplyNetworkOpFail
		}

		tunnel.rpcClient = rpc.NewClient(tunnel.rpcConn)
	}
	message.Tag |= MsgResident

	// DON'T need to check len(logs) == 0. It may be a reasonable
	// probe request sending
	var reply int64
	err = tunnel.rpcClient.Call("TunnelRPC.Transfer", message.TMessage, &reply)

	if err != nil {
		LOG.Error("Remote rpc server send error[%v]", err)
		// error is from network or rpc system.
		tunnel.rpcClient.Close()
		tunnel.rpcConn.Close()
		tunnel.rpcConn = nil
		return ReplyError
	}
	return reply
}

func (tunnel *RPCWriter) Prepare() bool {
	var address *net.TCPAddr
	var conn *net.TCPConn
	var err error
	if address, err = net.ResolveTCPAddr("tcp", tunnel.RemoteAddr); err != nil {
		LOG.Critical("Resolve rpc server address failed. %v", err)
		return false
	}
	tunnel.tcpAddr = address

	// check connection on initial stage
	if !InitialStageChecking {
		return true
	}

	if conn, err = net.DialTCP("tcp", nil, address); err != nil {
		LOG.Critical("Remote rpc server connect failed. %v", err)
		return false
	}
	// just test the connection
	conn.Close()

	return true
}

func (tunnel *RPCWriter) AckRequired() bool {
	return true
}

func (tunnel *RPCWriter) ParsedLogsRequired() bool {
	return false
}
