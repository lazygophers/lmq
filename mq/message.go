package mq

import (
	"encoding/binary"
	"errors"
	"github.com/aofei/sandid"
	"github.com/lazygophers/log"
	"io"
)

const (
	idLen     = 16
	idxLen    = 44
	itemBegin = uint16(0x1234)
	itemEnd   = uint16(0x5678)
)

var ErrDataFail = errors.New("data fail")

type MessageId [idLen]byte

type Message struct {
	Id        MessageId `json:"id"`
	CreatedAt int64     `json:"created_at"`
	Data      []byte    `json:"data"`

	offset  int64
	dataLen int64
}

func GenMessageId() (m MessageId) {
	copy(m[:], sandid.New().String())
	return

}

// 存储.idx文件
func (p *Message) WriteTo(w io.Writer) (int, error) {
	b := binary.LittleEndian
	bt := make([]byte, idxLen)
	log.Infof("msg offset: %d, msg datalen: %d", p.offset, p.dataLen)
	/*
		0-2: begin标识符
		2-18: message id
		18-26: created_at
		26-34: data的起始位置
		34-42: message data大小
		42-44: end标识符
	*/
	b.PutUint16(bt[0:2], itemBegin)
	copy(bt[2:18], p.Id[:])
	b.PutUint64(bt[18:26], uint64(p.CreatedAt))
	b.PutUint64(bt[26:34], uint64(p.offset))
	b.PutUint64(bt[34:42], uint64(p.dataLen))
	b.PutUint16(bt[42:44], itemEnd)

	return w.Write(bt)
}

func (p *Message) ReadFrom(r io.Reader) (n int, err error) {
	b := binary.LittleEndian
	bt := make([]byte, idxLen)

	n, err = r.Read(bt)
	if err != nil {
		return 0, err
	}

	if b.Uint16(bt[0:2]) != itemBegin {
		return 0, ErrDataFail
	}

	copy(p.Id[:], bt[2:18])
	p.CreatedAt = int64(b.Uint64(bt[18:26]))
	p.offset = int64(b.Uint64(bt[26:34]))
	p.dataLen = int64(b.Uint64(bt[34:42]))
	if b.Uint16(bt[42:44]) != itemEnd {
		return 0, ErrDataFail
	}
	return n, err
}
