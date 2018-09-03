package collector

import (
	"mongoshake/collector/configure"
	"mongoshake/common"
	"mongoshake/modules"
	"mongoshake/oplog"
	"mongoshake/tunnel"
)

type WriteController struct {
	// worker (not owned)
	worker *Worker

	// modules
	moduleList []Module

	// backend tunnel
	tunnel tunnel.Writer
	// current max lsn_ack value
	LatestLsnAck int64
}

type Module interface {
	IsRegistered() bool

	/**
	 * Module install and initialize. return false on failed
	 * and only invocation on WriteController is preparing
	 */
	Install() bool

	/**
	 * Handle outstanding request message. and messages
	 * are passed one by one. Any changes of message in
	 * Handle() will be preserved and delivery to next
	 *
	 * @return tunnel's error code (<0) or ack value
	 *
	 */
	Handle(message *tunnel.WMessage) int64
}

// the order of controller modules declared strictly
// doesn't change the order
var orderedModuleList = []Module{
	&module.Compressor{},
	&module.ChecksumCalculator{},
}

func NewWriteController(worker *Worker) *WriteController {
	writeController := &WriteController{worker: worker}
	if !writeController.installModules() {
		return nil
	}

	// create t by options
	factory := tunnel.WriterFactory{Name: conf.Options.Tunnel}
	if writeController.tunnel = factory.Create(conf.Options.TunnelAddress, worker.id); writeController.tunnel != nil {
		if writeController.tunnel.Prepare() {
			return writeController
		}
	}
	return nil
}

func (controller *WriteController) installModules() bool {
	for _, m := range orderedModuleList {
		if m.IsRegistered() {
			if !m.Install() {
				return false
			}
			controller.moduleList = append(controller.moduleList, m)
		}
	}
	return true
}

func (controller *WriteController) Send(logs []*oplog.GenericOplog, tag uint32) int64 {
	// all tunnel message which contain empty logs will be considered as
	// probe message. Include real probe to get ack from remote server
	// or a normal message doesn't have logs (which submit by retransmission)
	if !controller.tunnel.AckRequired() && tag&tunnel.MsgProbe != 0 {
		// probe message is not useful while tunnel AckRequired() is false
		// ignore these messages. Tag on these message contain
		// MsgRetransmission flag is impossible.
		return controller.LatestLsnAck
	}

	message := &tunnel.WMessage{
		TMessage: &tunnel.TMessage {
			Tag:        tag,
			Shard:      controller.worker.id,
			RawLogs:    oplog.LogEntryEncode(logs),
		},
		ParsedLogs: oplog.LogParsed(logs),
	}
	for _, m := range controller.moduleList {
		if internalCode := m.Handle(message); internalCode < 0 {
			return internalCode
		}
	}

	// we return the error directly if send() failed(feedback must less than zero). feedback bigger
	// or equal zero means has sent successfully. And if tunnel is AckRequired() we set the LatestLsnAck
	// in order to notify the upper layer ACK value. if not, we only drop the ACK and return the
	// "last message" timestamp. that indicates nothing should be ACKed
	if feedback := controller.tunnel.Send(message); feedback < 0 {
		// failed
		return feedback
	} else if controller.tunnel.AckRequired() {
		// ok, need ack value
		controller.LatestLsnAck = feedback
	} else if message.Tag&tunnel.MsgProbe == 0 && len(message.RawLogs) != 0 {
		// direct tunnel way will also come into this branch
		controller.LatestLsnAck = utils.TimestampToInt64(logs[len(logs)-1].Parsed.Timestamp)
	}
	// accumulated overall logs size
	controller.worker.syncer.replMetric.AddTunnelTraffic(message.ApproximateSize())

	return controller.LatestLsnAck
}
