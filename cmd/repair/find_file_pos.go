package repair

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/rolandhe/smss/binlog"
	"github.com/rolandhe/smss/standard"
	"github.com/rolandhe/smss/store/fss"
	"io"
	"log"
	"os"
	"path"
)

func FindBinlogPosByMessageId(ppath string, msgId int64) (int64, int64, error) {
	return findPosByMessageId(ppath, msgId, func(cmdBuf []byte) (int64, int) {
		cmd := binlog.CmdDecoder(cmdBuf)
		return cmd.MessageSeqId, cmd.PayloadLen
	})
}

func FindMqPosByMessageId(ppath string, msgId int64) (int64, int64, error) {
	return findPosByMessageId(ppath, msgId, func(cmdBuf []byte) (int64, int) {
		cmd := &fss.MqMessageCommand{}
		err := fss.ReadMqMessageCmd(cmdBuf[:len(cmdBuf)-1], cmd)
		if err != nil {
			return -1, -1
		}
		return cmd.GetId(), cmd.GetPayloadSize()
	})
}

func findPosByMessageId(ppath string, msgId int64, cmdExtractFunc func(cmdBuf []byte) (int64, int)) (int64, int64, error) {
	maxLogFileId, err := standard.ReadMaxFileId(ppath)
	if err != nil {
		return 0, 0, err
	}
	maxLogFileId--
	if maxLogFileId < 0 {
		return 0, 0, nil
	}
	curFileId := maxLogFileId

	for curFileId >= 0 {
		p := path.Join(ppath, fmt.Sprintf("%d.log", curFileId))
		info, err := os.Stat(p)
		if err != nil && os.IsNotExist(err) {
			return 0, 0, errors.New("can't find message id")
		}
		if err != nil {
			return 0, 0, err
		}
		found, findPos, err := findInFile(p, msgId, cmdExtractFunc)
		if err != nil {
			return 0, 0, err
		}
		if found == okFound {
			if findPos == info.Size() {
				return curFileId + 1, 0, nil
			}
			return curFileId, findPos, nil
		}
		curFileId--
	}
	return 0, 0, errors.New("can't find message id")
}

type foundEnum int

const (
	defaultFound foundEnum = 0
	okFound      foundEnum = 1
	needNext     foundEnum = 2
)

func findInFile(p string, msgId int64, extractCmd func(cmdBuf []byte) (int64, int)) (foundEnum, int64, error) {
	f, err := os.Open(p)
	if err != nil {
		return defaultFound, 0, err
	}
	defer f.Close()
	r := bufio.NewReader(f)

	cmdCommonSize := 512

	buf := make([]byte, cmdCommonSize)

	var nextPos int64
	first := true

	for {
		if _, err = io.ReadFull(r, buf[:4]); err != nil {
			return defaultFound, 0, err
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
			return defaultFound, 0, err
		}

		idInCmd, payloadLen := extractCmd(cBuf)
		nextPos += int64(cmdLen + 4 + payloadLen)
		if idInCmd == msgId {
			return okFound, nextPos, nil
		}

		if first {
			first = false
			if idInCmd > msgId {
				return needNext, 0, nil
			}
		}
		var discard int
		discard, err = r.Discard(payloadLen)
		if err != nil {
			return defaultFound, 0, err
		}
		if discard != payloadLen {
			log.Printf("invalid file:%s, expect:%d,but  discard %d err\n", p, payloadLen, discard)
			return defaultFound, 0, errors.New("invalid file")
		}

	}
	return defaultFound, 0, errors.New("can't find message id")
}
