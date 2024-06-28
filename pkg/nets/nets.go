package nets

import (
	"encoding/binary"
	"errors"
	"github.com/rolandhe/smss/cmd/protocol"
	"log"
	"net"
	"time"
)

func ReadAll(conn net.Conn, buff []byte) error {
	rb := buff[:]
	all := 0
	for {
		n, err := conn.Read(rb)
		if err != nil {
			return err
		}
		if n == -1 {
			return errors.New("peer closed")
		}
		all += n
		if all == len(buff) {
			break
		}
		rb = rb[n:]
	}
	return nil
}

func WriteAll(conn net.Conn, buf []byte) error {
	for {
		l := len(buf)
		n, err := conn.Write(buf)
		if err != nil {
			return err
		}
		if n == l {
			return nil
		}
		buf = buf[n:]
	}
}

func OutputRecoverErr(conn net.Conn, errMsg string) error {
	buf := make([]byte, protocol.RespHeaderSize)
	binary.LittleEndian.PutUint16(buf, protocol.ErrCode)
	l := len(errMsg)
	binary.LittleEndian.PutUint16(buf[2:], uint16(l))
	buf = append(buf, []byte(errMsg)...)

	if err := WriteAll(conn, buf); err != nil {
		log.Printf("outputRecoverErr,write to err msg conn err,%s\v", err)
		return err
	}
	return nil
}

func OutputOk(conn net.Conn) error {
	buf := make([]byte, protocol.RespHeaderSize)
	binary.LittleEndian.PutUint16(buf, protocol.OkCode)
	if err := WriteAll(conn, buf); err != nil {
		log.Printf("outputRecoverErr,write code to conn err,%s\v", err)
		return err
	}
	return nil
}

func OutAlive(conn net.Conn, timeout time.Duration) error {
	buf := make([]byte, protocol.RespHeaderSize)
	binary.LittleEndian.PutUint16(buf, protocol.AliveCode)
	conn.SetWriteDeadline(time.Now().Add(timeout))
	return WriteAll(conn, buf)
}

func OutSubEnd(conn net.Conn, timeout time.Duration) error {
	buf := make([]byte, protocol.RespHeaderSize)
	binary.LittleEndian.PutUint16(buf, protocol.SubEndCode)
	conn.SetWriteDeadline(time.Now().Add(timeout))
	return WriteAll(conn, buf)
}

func InputAck(conn net.Conn, timeout time.Duration) (int, error) {
	conn.SetReadDeadline(time.Now().Add(timeout))
	buf := make([]byte, 2)
	if err := ReadAll(conn, buf); err != nil {
		return 0, err
	}

	code := binary.LittleEndian.Uint16(buf)

	return int(code), nil
}

func IsTimeoutError(err error) bool {
	// 检查是否为 net.Error 类型并且是否为超时错误
	if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		return true
	}
	return false
}
