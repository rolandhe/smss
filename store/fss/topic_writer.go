package fss

import (
	"bytes"
	"github.com/rolandhe/smss/conf"
	"github.com/rolandhe/smss/standard"
	"os"
	"sync"
)

type topicWriter struct {
	*standard.StdMsgWriter[wrappedMsges]
	sync.WaitGroup
}

func newWriter(topicName, topicPath string) *topicWriter {
	w := &topicWriter{
		StdMsgWriter: standard.NewMsgWriter[wrappedMsges](topicName, topicPath, conf.MaxLogSize, buildWriteFunc()),
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
