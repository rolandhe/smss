package replica

import (
	"encoding/binary"
	"github.com/rolandhe/smss/binlog"
	"github.com/rolandhe/smss/cmd/protocol"
	"github.com/rolandhe/smss/conf"
	"github.com/rolandhe/smss/standard"
	"github.com/rolandhe/smss/store"
)

type binlogBlock struct {
	data []byte
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
	msgLen := len(p.cmdBuf) + len(payload) + 5
	buf := make([]byte, msgLen+protocol.RespHeaderSize)
	tmp := buf
	binary.LittleEndian.PutUint16(tmp, protocol.OkCode)
	binary.LittleEndian.PutUint32(tmp[2:], uint32(msgLen))
	tmp = tmp[protocol.RespHeaderSize:]

	binary.LittleEndian.PutUint32(buf, uint32(len(p.cmdBuf)+1))
	tmp = tmp[4:]
	n := copy(tmp, p.cmdBuf)
	tmp = tmp[n:]
	tmp[0] = '\n'
	copy(tmp[1:], payload)

	return &binlogBlock{
		data: buf,
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
	return c.cmd.MessageSeqId
}

type serverRegister struct {
	walMonitor WalMonitorSupport
	uuid       string
}

func (reg *serverRegister) RegisterReaderNotify(notify *standard.NotifyDevice) (standard.LogFileInfoGet, error) {
	return reg.walMonitor.RegNotify(reg.uuid, notify)
}
func (reg *serverRegister) UnRegisterReaderNotify() {
	reg.walMonitor.UnRegNotify(reg.uuid)
}
