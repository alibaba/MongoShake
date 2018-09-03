package module

import (
	"mongoshake/tunnel"

	LOG "github.com/vinllen/log4go"
)

/*
 * ====== ChecksumCoder =======
 *
 */
type ChecksumCalculator struct{}

func (coder *ChecksumCalculator) IsRegistered() bool {
	return true
}

func (coder *ChecksumCalculator) Install() bool {
	return true
}

func (coder *ChecksumCalculator) Handle(message *tunnel.WMessage) int64 {
	// write checksum value
	if len(message.RawLogs) != 0 {
		message.Checksum = message.Crc32()
		LOG.Debug("Tunnel message checksum value 0x%x", message.Checksum)
	}

	return tunnel.ReplyOK
}
