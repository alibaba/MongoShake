package tunnel

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"

	"github.com/gugemichael/nimo4go"
	LOG "github.com/vinllen/log4go"
)

type TCPReader struct {
	// listen
	listenAddress string
	// for golang tcp socket
	channel [2]*ListenSocket

	replayer []Replayer
	ack      int64
}

type ListenSocket struct {
	addr     *net.TCPAddr
	listener *net.TCPListener
}

func (reader *TCPReader) Link(replayer []Replayer) (err error) {
	reader.replayer = replayer
	for i := 0; i != TotalQueueNum; i++ {
		reader.channel[i] = new(ListenSocket)
		reader.channel[i].addr, err = net.ResolveTCPAddr("tcp4", reader.listenAddress)
		if err != nil {
			LOG.Critical("Resolve channel listenAddress error: %s", err.Error())
			return err
		}
	}
	reader.channel[RecvAckChannel].addr.Port = reader.channel[RecvAckChannel].addr.Port + 1
	for i := 0; i != TotalQueueNum; i++ {
		reader.channel[i].listener, err = net.ListenTCP("tcp", reader.channel[i].addr)
		if err != nil {
			LOG.Critical("Tcp reader server listen %v error: %s", reader.channel[i].addr, err.Error())
			return err
		}
	}

	// fork listen acceptor for oplog transfer tunnel
	nimo.GoRoutineInLoop(func() {
		socket, err := reader.channel[TransferChannel].listener.AcceptTCP()
		if err != nil {
			LOG.Warn("Server accept channel error : %s", err.Error())
			return
		}
		socket.SetNoDelay(false)
		socket.SetLinger(0)
		socket.SetReadBuffer(1024 * 1024 * 16)
		nimo.GoRoutine(func() {
			reader.recvTransfer(socket)
		})
	})

	// fork listen acceptor for ack value query tunnel
	nimo.GoRoutineInLoop(func() {
		socket, err := reader.channel[RecvAckChannel].listener.AcceptTCP()
		if err != nil {
			LOG.Warn("Server ACK accept ch error : %s", err.Error())
			return
		}
		socket.SetNoDelay(true)
		socket.SetLinger(0)
		nimo.GoRoutine(func() {
			reader.recvGetAck(socket)
		})
	})
	return nil
}

func (reader *TCPReader) recvTransfer(socket *net.TCPConn) {
	defer socket.Close()
	// every entire packet just for one loop time
	header := [HeaderLen]byte{}
	for {
		socketTimeout(socket, NetworkDefaultTimeout*10)
		// read util entire header
		if _, err := io.ReadAtLeast(socket, header[:], HeaderLen); err != nil {
			LOG.Warn("Server transfer read header at least failed readAtLeast %d, %s",
				HeaderLen, err.Error())
			return
		}
		packet := NewPacketV1(PacketIncomplete, nil)
		if !packet.decodeHeader(header[:]) {
			LOG.Warn("Server transfer decode header failed")
			return
		}
		nimo.AssertTrue(packet.typeOf == PacketWrite && packet.length != 0, "transfer receive bad type packet")

		payload := make([]byte, packet.length)
		if _, err := io.ReadAtLeast(socket, payload, int(packet.length)); err != nil {
			LOG.Warn("Server transfer read packet at least failed readAtLeast %d, %s",
				packet.length, err.Error())
			return
		}
		message := new(TMessage)
		message.FromBytes(payload, binary.BigEndian)

		// hash corresponding replayer and re-sharding
		if message.Shard >= uint32(len(reader.replayer)) {
			message.Shard %= uint32(len(reader.replayer))
		}
		reader.ack = reader.replayer[message.Shard].Sync(message, nil)
	}
}

func (reader *TCPReader) recvGetAck(socket *net.TCPConn) {
	defer socket.Close()
	// every entire packet just for one loop time
	header := [HeaderLen]byte{}
	for {
		socketTimeout(socket, NetworkDefaultTimeout)
		// read util entire header
		if _, err := io.ReadAtLeast(socket, header[:], HeaderLen); err != nil {
			LOG.Warn("Server ack read header at least failed readAtLeast %d, %s",
				HeaderLen, err.Error())
			return
		}
		packet := NewPacketV1(PacketIncomplete, nil)
		if !packet.decodeHeader(header[:]) {
			LOG.Warn("Server ack decode header failed")
			return
		}
		nimo.AssertTrue(packet.typeOf == PacketGetACK && packet.length == 0, "ack receive bad type packet")

		// write back ack
		buffer := &bytes.Buffer{}
		binary.Write(buffer, binary.BigEndian, reader.ack)
		packet = NewPacketV1(PacketReturnACK, buffer.Bytes())
		if _, err := socket.Write(packet.encode()); err != nil {
			if err, ok := err.(net.Error); ok && err.Timeout() {
				LOG.Warn("Tcp ack send ack back timeout")
			}
		}
		nimo.AssertTrue(packet.length != 0, "ack send bad PacketReturnACK packet len")
	}
}
