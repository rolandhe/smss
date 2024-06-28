package standard

import (
	"encoding/binary"
	"errors"
	"syscall"
)

type commandStep struct {
	cmdBuff  [512]byte
	winCount int
	pos      int
	cmdLen   int
}

func (cs *commandStep) getCmdBuf() []byte {
	return cs.cmdBuff[4 : cs.cmdLen+4]
}

func (cs *commandStep) accept(rctx *readContext) (bool, error) {
	inputData := rctx.data
	for len(inputData) > 0 {
		if cs.pos < 4 {
			n := copy(cs.cmdBuff[cs.pos:4], inputData)
			inputData = inputData[n:]
			cs.pos += n
			continue
		}
		if cs.pos == 4 {
			cs.cmdLen = int(binary.LittleEndian.Uint32(cs.cmdBuff[:4]))
			n := copy(cs.cmdBuff[cs.pos:cs.cmdLen+4], inputData)
			cs.pos += n
			inputData = inputData[n:]
			continue
		}
		if cs.pos == cs.cmdLen+4 {
			break
		}
		n := copy(cs.cmdBuff[cs.pos:cs.cmdLen+4], inputData)
		cs.pos += n
		inputData = inputData[n:]
	}

	if cs.pos > 4 && cs.pos == cs.cmdLen+4 {
		readCount := len(rctx.data) - len(inputData)
		rctx.consumeData(readCount)
		return true, nil
	}
	if cs.winCount > 0 {
		return false, errors.New("no cmd line, invalid pos")
	}
	cs.winCount++
	rctx.consumeData(len(rctx.data))
	return false, nil
}

func (cs *commandStep) reset() {
	cs.winCount = 0
	cs.cmdLen = 0
	cs.pos = 0
}

type payloadStep struct {
	payload     []byte
	size        int
	headerCount int
	pos         int
}

// accept  has next msg
func (ps *payloadStep) accept(rctx *readContext) (bool, error) {
	n := copy(ps.payload[ps.pos:], rctx.data)
	ps.pos += n
	rctx.consumeData(n)

	if ps.pos == ps.size {
		return true, nil
	}

	return false, nil
}

func (ps *payloadStep) reset() {
	ps.payload = nil
	ps.size = 0
	ps.pos = 0
}

type readContext struct {
	fd   uintptr
	pos  int64
	data []byte
}

func (ctx *readContext) consumeData(size int) {
	ctx.pos += int64(size)
	if size == len(ctx.data) {
		ctx.data = nil
		return
	}
	ctx.data = ctx.data[size:]
}

func (ctx *readContext) next(fileSize int64) error {
	if len(ctx.data) > 0 {
		return nil
	}
	index := ctx.pos / MMapWindow
	offset := ctx.pos % MMapWindow
	winSize := MMapWindow
	start := index * MMapWindow

	if (fileSize - start) < winSize {
		winSize = fileSize - start
	}
	data, err := syscall.Mmap(int(ctx.fd), start, int(winSize), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return err
	}
	ctx.data = data[offset:]
	return nil
}
