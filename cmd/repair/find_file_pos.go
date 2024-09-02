package repair

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/rolandhe/smss/binlog"
	"github.com/rolandhe/smss/conf"
	"github.com/rolandhe/smss/pkg/logger"
	"github.com/rolandhe/smss/pkg/tm"
	"github.com/rolandhe/smss/standard"
	"github.com/rolandhe/smss/store/fss"
	"io"
	"os"
	"path"
)

func FindBinlogPosByEventId(ppath string, eventId int64, lastFileId int64) (int64, int64, error) {
	return findPosByEventId(ppath, eventId, lastFileId, func(cmdBuf []byte) (int64, int) {
		cmd := binlog.CmdDecoder(cmdBuf)
		return cmd.EventId, cmd.PayloadLen
	})
}

func FindTopicPosByEventId(ppath string, eventId int64, lastFileId int64) (int64, int64, error) {
	return findPosByEventId(ppath, eventId, lastFileId, func(cmdBuf []byte) (int64, int) {
		cmd := &fss.TopicMessageCommand{}
		err := fss.ReadTopicMessageCmd(cmdBuf[:len(cmdBuf)-1], cmd)
		if err != nil {
			logger.Infof("FindTopicPosByEventId for %d err:%v", eventId, err)
			return -1, -1
		}
		return cmd.GetId(), cmd.GetPayloadSize()
	})
}

func findPosByEventId(ppath string, eventId int64, lastFileId int64, cmdExtractFunc func(cmdBuf []byte) (int64, int)) (int64, int64, error) {
	maxLogFileId, err := standard.ReadMaxFileId(ppath)
	if err != nil {
		return 0, 0, err
	}
	maxLogFileId--
	if maxLogFileId < 0 {
		return 0, 0, nil
	}

	nowDate := tm.NowDate()

	lastExpired := false

	for curFileId := maxLogFileId; curFileId >= 0; curFileId-- {
		p := path.Join(ppath, fmt.Sprintf("%d.log", curFileId))
		stat, err := os.Stat(p)
		if err != nil && os.IsNotExist(err) {
			return 0, 0, errors.New("can't find event id,because file not exist")
		}
		if err != nil {
			return 0, 0, err
		}

		modDate := tm.ToDate(stat.ModTime())
		expired := tm.DiffDays(nowDate, modDate) > conf.StoreMaxDays

		found, findPos, err := findInFile(p, eventId, cmdExtractFunc)
		if err != nil {
			return 0, 0, err
		}
		if found == okFound {
			if expired {
				if lastExpired {
					return 0, 0, errors.New("found event id,but expired")
				}
				if lastFileId > curFileId && findPos != stat.Size() {
					return 0, 0, errors.New("found event id,but expired")
				}
			}
			return curFileId, findPos, nil
		}
		lastExpired = expired
	}
	return 0, 0, errors.New("can't find event id")
}

type foundEnum int

const (
	defaultFound foundEnum = 0
	okFound      foundEnum = 1
	needNext     foundEnum = 2
)

func findInFile(p string, eventId int64, extractCmd func(cmdBuf []byte) (int64, int)) (foundEnum, int64, error) {
	f, err := os.Open(p)
	if err != nil {
		return defaultFound, 0, err
	}
	defer f.Close()
	r := bufio.NewReader(f)

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
		if idInCmd == eventId {
			return okFound, nextPos, nil
		}

		if first {
			first = false
			if idInCmd > eventId {
				return needNext, 0, nil
			}
		}
		var discard int
		discard, err = r.Discard(payloadLen)
		if err != nil {
			return defaultFound, 0, err
		}
		if discard != payloadLen {
			logger.Infof("invalid file:%s, expect:%d,but  discard %d err", p, payloadLen, discard)
			return defaultFound, 0, errors.New("invalid file")
		}

	}
	return defaultFound, 0, errors.New("can't find event id")
}
