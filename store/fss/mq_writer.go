package fss

import (
	"bytes"
	"github.com/rolandhe/smss/conf"
	"github.com/rolandhe/smss/standard"
	"os"
	"sync"
)

type mqWriter struct {
	*standard.StdMsgWriter[wrappedMsges]
	sync.WaitGroup
}

func newWriter(mqName, mqPath string) *mqWriter {
	w := &mqWriter{
		StdMsgWriter: standard.NewMsgWriter[wrappedMsges](mqName, mqPath, conf.MaxLogSize, buildWriteFunc()),
	}
	return w
}

func buildWriteFunc() standard.OutputMsgFunc[wrappedMsges] {
	return func(f *os.File, amsg *wrappedMsges) (int64, error) {
		cmds, size := buildCommandsAndCalcSize(amsg)
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
