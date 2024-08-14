package replica

import (
	"encoding/binary"
	"github.com/rolandhe/smss/binlog"
	"github.com/rolandhe/smss/cmd/protocol"
	"github.com/rolandhe/smss/conf"
	"github.com/rolandhe/smss/pkg/logger"
	"github.com/rolandhe/smss/standard"
	"github.com/rolandhe/smss/store"
)

type binlogBlock struct {
	data   []byte
	rawMsg *protocol.RawMessage
}
type serverBlockReader struct {
	*standard.StdMsgBlockReader[binlogBlock]
}

type serverBinlogBlockReader interface {
	store.BlockReader[binlogBlock]
}

func newBlockReader(uuidStr string, walMonitor WalMonitorSupport) serverBinlogBlockReader {
	r := standard.NewStdMsgBlockReader[binlogBlock]("binlog", walMonitor.GetRoot(), uuidStr, 1, conf.MaxLogSize, &serverRegister{
		walMonitor: walMonitor,
		uuid:       uuidStr,
	}, &msgParser{})
	return &serverBlockReader{
		StdMsgBlockReader: r,
	}
}

type msgParser struct {
	cmdBuf []byte
	cmd    *protocol.DecodedRawMessage
}

func (p *msgParser) ToMessage(payload []byte, fileId, pos int64) *binlogBlock {
	msgLen := len(p.cmdBuf) + len(payload) + 4
	buf := make([]byte, msgLen)

	binary.LittleEndian.PutUint32(buf, uint32(len(p.cmdBuf)))
	tmp := buf[4:]
	n := copy(tmp, p.cmdBuf)
	tmp = tmp[n:]
	copy(tmp, payload)

	return &binlogBlock{
		data:   buf,
		rawMsg: &p.cmd.RawMessage,
	}
}
func (p *msgParser) Reset() {
	p.cmdBuf = nil
	p.cmd = nil
}

func (p *msgParser) ChangeMessagePos(ins *binlogBlock, fileId, pos int64) {

}
func (p *msgParser) ParseCmd(cmdBuf []byte) (standard.CmdLine, error) {
	p.cmdBuf = cmdBuf
	p.cmd = binlog.CmdDecoder(cmdBuf)
	line := &binlogCmd{
		cmd: p.cmd,
	}
	return line, nil
}

type binlogCmd struct {
	cmd *protocol.DecodedRawMessage
}

func (c *binlogCmd) GetPayloadSize() int {
	return c.cmd.PayloadLen
}
func (c *binlogCmd) GetCmd() protocol.CommandEnum {
	return c.cmd.Command
}
func (c *binlogCmd) GetId() int64 {
	return c.cmd.EventId
}

type serverRegister struct {
	walMonitor WalMonitorSupport
	uuid       string
}

func (reg *serverRegister) RegisterReaderNotify(notify *standard.NotifyDevice) (standard.LogFileInfoGet, error) {
	infoGet, err := reg.walMonitor.RegNotify(reg.uuid, notify)
	logger.Get().Infof("serverRegister.RegisterReaderNotify, %s,err:%v", reg.uuid, err)
	return infoGet, err
}
func (reg *serverRegister) UnRegisterReaderNotify() {
	reg.walMonitor.UnRegNotify(reg.uuid)
	logger.Get().Infof("serverRegister.UnRegisterReaderNotify, %s", reg.uuid)
}
