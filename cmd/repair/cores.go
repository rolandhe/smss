package repair

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/rolandhe/smss/binlog"
	"github.com/rolandhe/smss/cmd/protocol"
	"github.com/rolandhe/smss/standard"
	"github.com/rolandhe/smss/store"
	"github.com/rolandhe/smss/store/fss"
	"io"
	"log"
	"os"
	"path"
)

var repairHandlers = map[protocol.CommandEnum]func(lBinlog *lastBinlog, binlogFile, dataRoot string, meta store.Meta) error{}

func init() {
	repairHandlers[protocol.CommandPub] = repairPub
	repairHandlers[protocol.CommandCreateMQ] = repairCreate
	repairHandlers[protocol.CommandDeleteMQ] = repairDelete
	repairHandlers[protocol.CommandChangeLf] = repairChangLf
	repairHandlers[protocol.CommandDelay] = repairDelay
	repairHandlers[protocol.CommandDelayApply] = repairDelayApply
}

func ensureLogFile(ppath string) (string, int64, int64, error) {
	maxLogFileId, err := standard.ReadMaxFileId(ppath)
	if err != nil {
		return "", 0, 0, err
	}
	maxLogFileId--
	if maxLogFileId < 0 {
		return "", 0, 0, nil
	}
	var p string
	var fileSize int64
	p, maxLogFileId, fileSize, err = ensureNoEmptyLog(ppath, maxLogFileId)
	if err != nil {
		return "", 0, 0, err
	}

	return p, maxLogFileId, fileSize, nil
}

//func readAllBuf(buf []byte, r *bufio.Reader) error {
//	needLen := len(buf)
//
//	count := 0
//	for {
//		n, err := r.Read(buf)
//		if err != nil {
//			return err
//		}
//		needLen -= n
//		//if count > 0 {
//		//	log.Println(1)
//		//}
//		if needLen == 0 {
//			break
//		}
//		count++
//		buf = buf[n:]
//	}
//	//n, err := r.Read(buf)
//	//if err != nil {
//	//	return err
//	//}
//	//if n != len(buf) {
//	//	return errors.New("bad file, no enough")
//	//}
//	return nil
//}

type extractLog[C, T any] interface {
	extractCmd(cmdBuf []byte) (*C, int)
	extractRet(cmd *C, pos int64, payload []byte) *T
}

type lastBinlog struct {
	fileId       int64
	pos          int64
	cmd          protocol.CommandEnum
	messageSeqId int64
	mqName       string
	payload      []byte
}

type extractBinlog struct {
	fileId int64
}

func (ext *extractBinlog) extractCmd(cmdBuf []byte) (*protocol.DecodedRawMessage, int) {
	cmd := binlog.CmdDecoder(cmdBuf)
	return cmd, cmd.PayloadLen
}
func (ext *extractBinlog) extractRet(cmd *protocol.DecodedRawMessage, pos int64, payload []byte) *lastBinlog {
	last := &lastBinlog{
		fileId:       ext.fileId,
		pos:          pos,
		mqName:       cmd.MqName,
		cmd:          cmd.Command,
		messageSeqId: cmd.EventId,
		payload:      payload,
	}
	return last
}

type lastMqLog struct {
	id           int64
	cmdLen       int
	pos          int64
	indexOfBatch int
	srcFileId    int64
	srcPos       int64
}

type extractMqLog struct {
}

func (ext *extractMqLog) extractCmd(cmdBuf []byte) (*fss.MqMessageCommand, int) {
	cmd := &fss.MqMessageCommand{}
	err := fss.ReadMqMessageCmd(cmdBuf[:len(cmdBuf)-1], cmd)
	if err != nil {
		return nil, 0
	}
	return cmd, cmd.GetPayloadSize()
}
func (ext *extractMqLog) extractRet(cmd *fss.MqMessageCommand, pos int64, payload []byte) *lastMqLog {
	last := &lastMqLog{
		id:           cmd.GetId(),
		cmdLen:       cmd.CmdLen,
		pos:          pos,
		indexOfBatch: cmd.IndexOfBatch,
		srcFileId:    cmd.SrcFileId,
		srcPos:       cmd.SrcPos,
	}
	return last
}

func readLastLogBlock[C, T any](startPosition int64, p string, fileSize int64, extractor extractLog[C, T]) (*T, error) {
	f, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := bufio.NewReader(f)

	cmdCommonSize := 512

	buf := make([]byte, cmdCommonSize)
	if startPosition >= fileSize {
		return nil, errors.New("small file")
	}
	if startPosition > 0 {
		var discard int
		if discard, err = r.Discard(int(startPosition)); err != nil {
			return nil, err
		}
		if discard != int(startPosition) {
			log.Printf("invalid start pos of %s, expect:%d, but:%d\n", p, startPosition, discard)
			return nil, errors.New("invalid start pos")
		}
	}

	var pos = startPosition
	var cmdLine *C
	var allSize int64
	var lastPayload []byte

	for {
		if _, err = io.ReadFull(r, buf[:4]); err != nil {
			return nil, err
		}

		cmdLen := int(binary.LittleEndian.Uint32(buf))

		var cBuf []byte
		if cmdLen <= cmdCommonSize {
			cBuf = buf[:cmdLen]
		} else {
			cBuf = make([]byte, cmdLen)
		}

		_, err = io.ReadFull(r, cBuf)
		if err != nil {
			return nil, err
		}

		var payloadLen int

		cmdLine, payloadLen = extractor.extractCmd(cBuf)

		allSize = int64(cmdLen+4) + int64(payloadLen)
		if pos+allSize > fileSize {
			return nil, errors.New("bad file")
		}
		if pos+allSize == fileSize {
			if payloadLen > 0 {
				lastPayload = make([]byte, payloadLen)
				_, err = io.ReadFull(r, lastPayload)
				if err != nil {
					return nil, err
				}
			}
			break
		}

		if payloadLen > 0 {
			var discard int
			discard, err = r.Discard(payloadLen)
			if err != nil {
				return nil, err
			}
			if discard != payloadLen {
				log.Printf("invalid file:%s, expect:%d,but  discard %d err\n", p, payloadLen, discard)
				return nil, errors.New("invalid file")
			}
		}

		pos += allSize

	}

	last := extractor.extractRet(cmdLine, pos, lastPayload)

	return last, nil
}

func ensureNoEmptyLog(binlogRoot string, fileId int64) (string, int64, int64, error) {
	last := ""
	for fileId >= 0 {
		p := path.Join(binlogRoot, fmt.Sprintf("%d.log", fileId))
		info, err := os.Stat(p)
		if err != nil && os.IsNotExist(err) {
			return "", 0, 0, nil
		}
		if err != nil {
			return "", 0, 0, err
		}
		if last != "" {
			if err = os.Remove(last); err != nil {
				return "", 0, 0, err
			}
			last = ""
		}
		if info.Size() > 0 {
			return p, fileId, info.Size(), nil
		}
		last = p
		fileId--
	}
	if fileId < 0 {
		if err := os.Remove(last); err != nil {
			return "", 0, 0, err
		}
	}
	return "", fileId, 0, nil
}

func maybeRemove(p string) error {
	info, err := os.Stat(p)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if info.Size() == 0 {
		return os.Remove(p)
	}
	return nil
}
