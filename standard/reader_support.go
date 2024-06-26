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

type buffedCmdData struct {
	datas [][]byte
}

func (bc *buffedCmdData) readCmd() ([]byte, int) {
	allLen := len(bc.datas[0]) + len(bc.datas[1])
	if allLen <= 4 {
		return nil, -1
	}
	readCount := 0
	lenBuf := make([]byte, 4)
	index := 0
	n := copy(lenBuf, bc.datas[index])
	readCount += n
	if n < 4 {
		index++
		n = copy(lenBuf[n:], bc.datas[index])
		readCount += n
	}
	cmdLen := int(binary.LittleEndian.Uint32(lenBuf))
	if allLen < cmdLen+4 {
		return nil, -1
	}
	cmdBuf := make([]byte, cmdLen)
	n = copy(cmdBuf, bc.datas[index][n:])
	readCount += n
	if n < cmdLen {
		index++
		n = copy(cmdBuf[n:], bc.datas[index])
		readCount += n
	}

	return cmdBuf[:cmdLen-1], readCount - len(bc.datas[0]) - 1
}

func (cs *commandStep) accept(rctx *readContext) (bool, error) {
	buffedData := buffedCmdData{
		datas: [][]byte{cs.cmdBuff, rctx.data},
	}

	cmdData, index := buffedData.readCmd()

	if index == -1 {
		if cs.winCount > 0 {
			return false, errors.New("no cmd line, invalid pos")
		}
		cs.cmdBuff = append(cs.cmdBuff, rctx.data...)
		cs.winCount++
		rctx.consumeData(len(rctx.data))
		return false, nil
	}
	cs.cmdBuff = nil
	cs.cmdBuff = append(cs.cmdBuff, cmdData...)
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
