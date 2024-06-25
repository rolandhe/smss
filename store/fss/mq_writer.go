package fss

import (
	"bytes"
	"github.com/rolandhe/smss/standard"
	"os"
	"sync"
)

type mqWriter struct {
	*standard.StdMsgWriter[asyncMsg]
	sync.WaitGroup
}

func newWriter(mqName, mqPath string, idGen func() (int64, error)) *mqWriter {
	w := &mqWriter{
		StdMsgWriter: standard.NewMsgWriter[asyncMsg](mqName, mqPath, MaxFileSize, buildWriteFunc(idGen)),
	}
	return w
}

func genIds(count int, idGen func() (int64, error)) ([]int64, error) {
	msgIds := make([]int64, count)
	var err error
	for i := 0; i < count; i++ {
		msgIds[i], err = idGen()
		if err != nil {
			return nil, err
		}
	}
	return msgIds, nil
}

func buildWriteFunc(idGen func() (int64, error)) standard.OutputMsgFunc[asyncMsg] {
	return func(f *os.File, amsg *asyncMsg) (int64, error) {
		msgIds, err := genIds(len(amsg.messages), idGen)
		if err != nil {
			return 0, err
		}
		cmds, size := buildCommandsAndCalcSize(msgIds, amsg)
		var buf bytes.Buffer
		buf.Grow(size)

		for i, msg := range amsg.messages {
			buf.Write(cmds[i])
			buf.Write(msg.Content)
			buf.WriteRune('\n')
		}

		return buf.WriteTo(f)
	}
}
