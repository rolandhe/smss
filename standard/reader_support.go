package standard

import (
	"encoding/binary"
	"errors"
	"syscall"
)

type commandStep struct {
	cmdBuff  []byte
	winCount int
}

func (cs *commandStep) accept(rctx *readContext) (bool, error) {
	index := -1
	dataLen := len(rctx.data)
	if dataLen >= 4 {
		cmdLen := int(binary.LittleEndian.Uint32(rctx.data))
		if dataLen >= cmdLen+4 {
			index = cmdLen + 4 - 1
		}
	}

	if index == -1 {
		if cs.winCount > 0 {
			return false, errors.New("no cmd line, invalid pos")
		}
		cs.cmdBuff = append(cs.cmdBuff, rctx.data...)
		cs.winCount++
		rctx.consumeData(len(rctx.data))
		return false, nil
	}
	cs.cmdBuff = append(cs.cmdBuff, rctx.data[4:index]...)
	rctx.consumeData(index + 1)
	return true, nil
}

func (cs *commandStep) reset() {
	cs.cmdBuff = nil
	cs.winCount = 0
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
