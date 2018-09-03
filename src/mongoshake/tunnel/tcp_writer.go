package tunnel

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"

	"mongoshake/common"

	LOG "github.com/vinllen/log4go"
	"github.com/gugemichael/nimo4go"
)

// Network packet structure
//
//		[ Big-edian ]
//		Header (12 Bytes)
// 		Body (n Bytes)
//
//		[ Header structure ]
//		-----------------------------------------------------------------------------------
//		|    magic(2B)    |  version(1B)  |  type(1B)  |  crc32(4B) |  length(4B)  |
//		-----------------------------------------------------------------------------------
//		|  0x00201314   |       0x01       |      0x01    |   0xFFFFF  |     4096        |
//		-----------------------------------------------------------------------------------
//
//		[ PacketWrite payload ]
//		-------------------------------------------------------------------------------------------------------------------------------------------------
//		|    cksum(4B)    |  tag(4B)  |  shard(4B)  |  compress(4B) |  number(4B)  |  len(4B)  |  log([]byte)  |  len(4B)  |  log([]byte)  |
//		-------------------------------------------------------------------------------------------------------------------------------------------------
//
//		[ PacketGetACK payload ]
//		--------------|
//		|    (zero)    |
//		--------------|
//
//		[ PacketReturnACK payload ]
//		------------------
//		|    ack(4B)    |
//		------------------
//
const (
	MagicNumber    = 0xCAFE
	CurrentVersion = 0x01
	HeaderLen      = 12
)

const (
	PacketIncomplete uint8 = 0x00
	PacketGetACK     uint8 = 0x01
	PacketWrite      uint8 = 0x02
	PacketReturnACK  uint8 = 0x3

	UndefinedPacketType uint8 = 0x4
)

const (
	TransferChannel = iota
	RecvAckChannel
	TotalQueueNum
)

const NetworkDefaultTimeout = 60 * time.Second

type Packet struct {
	magic   uint16
	version uint8
	typeOf  uint8
	crc32   uint32
	length  uint32

	payload []byte
}

func NewPacketV1(packetType uint8, payload []byte) *Packet {
	return &Packet{magic: MagicNumber, version: CurrentVersion, typeOf: packetType, length: uint32(len(payload)), payload: payload}
}

func (packet *Packet) setPayload(payload []byte) {
	packet.payload = payload
	packet.length = uint32(len(payload))
}

func (packet *Packet) encode() []byte {
	buffer := bytes.Buffer{}
	binary.Write(&buffer, binary.BigEndian, packet.magic)
	binary.Write(&buffer, binary.BigEndian, packet.version)
	binary.Write(&buffer, binary.BigEndian, packet.typeOf)
	// TODO: now crc32 is marked zero
	binary.Write(&buffer, binary.BigEndian, packet.crc32)
	binary.Write(&buffer, binary.BigEndian, packet.length)
	buffer.Write(packet.payload)
	nimo.AssertTrue(buffer.Len() == (HeaderLen+len(packet.payload)), "write packet header length is bad")
	return buffer.Bytes()
}

func (packet *Packet) decodeHeader(buffer []byte) bool {
	nimo.AssertTrue(len(buffer) == HeaderLen, "read packet header length is bad")
	buf := bytes.NewBuffer(buffer)
	binary.Read(buf, binary.BigEndian, &packet.magic)
	binary.Read(buf, binary.BigEndian, &packet.version)
	binary.Read(buf, binary.BigEndian, &packet.typeOf)
	binary.Read(buf, binary.BigEndian, &packet.crc32)
	binary.Read(buf, binary.BigEndian, &packet.length)
	return packet.valid()
}

func (packet *Packet) valid() bool {
	return packet.magic == MagicNumber && packet.version == CurrentVersion &&
		packet.typeOf < UndefinedPacketType
}

func (packet *Packet) String() string {
	return fmt.Sprintf("[magic:%d, ver:%d, type:%d, crc:%d, len:%d]",
		packet.magic, packet.version, packet.typeOf, packet.crc32, packet.length)
}

type TCPWriter struct {
	RemoteAddr string
	// for tcp stream channel
	channel [2]*TcpSocket

	ack int64
}

type TcpSocket struct {
	addr   *net.TCPAddr
	socket *net.TCPConn
}

func (tcp *TcpSocket) ensureNetwork() error {
	if tcp.socket == nil {
		var err error
		tcp.socket, err = net.DialTCP("tcp4", nil, tcp.addr)
		if err != nil {
			LOG.Critical("channel connect to %s error %s", tcp.addr.String(), err.Error())
			return err
		}
		tcp.socket.SetNoDelay(false)
		// linger policy is not required. our data kept in sender util acked
		tcp.socket.SetLinger(0)
		// default 16K. we set 16MB
		tcp.socket.SetWriteBuffer(1024 * 1024 * 16)
	}
	return nil
}

func (tcp *TcpSocket) release() {
	tcp.socket.Close()
	tcp.socket = nil
}

func (writer *TCPWriter) pollRemoteAckValue() {
	queryAck := NewPacketV1(PacketGetACK, nil).encode()
	header := [HeaderLen]byte{}
	tcp := writer.channel[RecvAckChannel]

	nimo.GoRoutineInLoop(func() {
		defer utils.DelayFor(1000)
		if tcp.ensureNetwork() != nil {
			return
		}

		// send get ack request
		socketTimeout(tcp.socket, NetworkDefaultTimeout)
		tcp.socket.Write(queryAck)
		// read util we got a entire header
		if _, err := io.ReadAtLeast(tcp.socket, header[:], HeaderLen); err != nil {
			tcpErrorAndRelease(tcp, err.Error())
			return
		}
		result := NewPacketV1(PacketIncomplete, nil)
		if !result.decodeHeader(header[:]) {
			tcpErrorAndRelease(tcp, "decode header failed")
			return
		}
		nimo.AssertTrue(result.typeOf == PacketReturnACK && result.length != 4, "acker receive bad type queryAck")

		// it's bad response if length < 4
		payload := make([]byte, result.length)
		if _, err := io.ReadAtLeast(tcp.socket, payload, int(result.length)); err != nil {
			tcpErrorAndRelease(tcp, err.Error())
			return
		}
		result.setPayload(payload)
		nimo.AssertTrue(result.length == uint32(len(result.payload)) && len(result.payload) != 0,
			"acker receive bad payload queryAck")
		binary.Read(bytes.NewBuffer(result.payload), binary.BigEndian, &writer.ack)
	})
}

func (writer *TCPWriter) Send(message *WMessage) int64 {
	tcp := writer.channel[TransferChannel]
	var err error
	if err = tcp.ensureNetwork(); err != nil {
		return ReplyNetworkOpFail
	}
	message.Tag |= MsgResident

	packet := NewPacketV1(PacketWrite, message.ToBytes(binary.BigEndian))
	// TODO: no timeout ??
	socketTimeout(tcp.socket, 0)
	if _, err = tcp.socket.Write(packet.encode()); err != nil {
		if err, ok := err.(net.Error); ok && err.Timeout() {
			LOG.Warn("Tcp writer send data packet timeout")
			return ReplyNetworkTimeout
		}
		tcp.release()
		return ReplyNetworkOpFail
	}
	return writer.ack
}

func (writer *TCPWriter) Prepare() bool {
	var err error
	writer.channel = [2]*TcpSocket{new(TcpSocket), new(TcpSocket)}
	for i := 0; i != TotalQueueNum; i++ {
		writer.channel[i].addr, err = net.ResolveTCPAddr("tcp4", writer.RemoteAddr)
		if err != nil {
			LOG.Critical("Resolve channel listenAddress error: %s", err.Error())
			return false
		}
	}
	writer.channel[RecvAckChannel].addr.Port = writer.channel[TransferChannel].addr.Port + 1
	// continuously update the ACK value via separate socket
	writer.pollRemoteAckValue()

	if !InitialStageChecking {
		return true
	}
	for _, ch := range writer.channel {
		if err = ch.ensureNetwork(); err != nil {
			return false
		}
	}
	return true
}

func (writer *TCPWriter) AckRequired() bool {
	return true
}

func (writer *TCPWriter) ParsedLogsRequired() bool {
	return false
}

func socketTimeout(socket *net.TCPConn, duration time.Duration) {
	if duration != 0 {
		socket.SetWriteDeadline(time.Now().Add(duration))
	}
}

func tcpErrorAndRelease(socket *TcpSocket, err string) {
	LOG.Critical("tcp operation error and release, %s", err)
	socket.release()
}
