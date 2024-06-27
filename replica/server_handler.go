package replica

import (
	"encoding/binary"
	"github.com/rolandhe/smss/cmd/protocol"
	"github.com/rolandhe/smss/pkg/nets"
	"github.com/rolandhe/smss/standard"
	"net"
)

func HandleServer(conn net.Conn, header *protocol.CommonHeader, binlogWriter *standard.StdMsgWriter[protocol.RawMessage]) error {
	buf := make([]byte, 16)
	err := nets.ReadAll(conn, buf)
	if err != nil {
		return err
	}
	fileId := int64(binary.LittleEndian.Uint64(buf))
	pos := int64(binary.LittleEndian.Uint64(buf[8:]))
	checkFile(binlogWriter.GetRoot(), fileId, pos)

	return nil
}

func checkFile(root string, fileId, pos int64) bool {
	return true
}
