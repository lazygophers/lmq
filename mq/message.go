package mq

import (
	"encoding/binary"
	"errors"
	"github.com/aofei/sandid"
	"github.com/lazygophers/log"
	"io"
	"time"
)

const (
	idLen     = 16
	idxLen    = 66
	itemBegin = uint16(0x1234)
	itemEnd   = uint16(0x5678)
)

var ErrDataFail = errors.New("data fail")

type MessageId [idLen]byte
type Priority uint8

const (
	PriorityLow    Priority = 0
	PriorityMiddle Priority = 128
	PriorityHigh   Priority = 255
)

type Message struct {
	Index int64 `json:"index,omitempty"`

	Id           MessageId `json:"id,omitempty"`
	CreatedAt    int64     `json:"created_at,omitempty"`
	Data         []byte    `json:"data,omitempty"`
	AccessExecAt int64     `json:"access_exec_at,omitempty"`
	Tag          uint64    `json:"tag,omitempty"`
	Priority     Priority  `json:"priority,omitempty"`

	RetryCount    uint8     `json:"retry_count,omitempty"`
	RunningExecAt time.Time `json:"running_exec_at,omitempty"`

	offset  int64
	dataLen int64
}

func GenMessageId() (m MessageId) {
	copy(m[:], sandid.New().String())
	return
}

/*
	Message 索引文件结构:
		0-2: begin标识符
		2-10: index
		10-26: message id
		26-34: created_at
		34-42: access_exec_at
		42-50: tag
		50-52: priority
		52-60: data的起始位置
		60-68: message data大小
		68-70: end标识符
*/

// WriteTo 存储.idx文件
func (p *Message) WriteTo(w io.Writer) (int, error) {
	b := binary.LittleEndian
	bt := make([]byte, idxLen)
	log.Infof("write msg,offset:%d,datalen:%d,tag:%d", p.offset, p.dataLen, p.Tag)

	b.PutUint16(bt[0:2], itemBegin)
	b.PutUint64(bt[2:10], uint64(p.Index))
	copy(bt[10:26], p.Id[:])
	b.PutUint64(bt[26:34], uint64(p.CreatedAt))
	b.PutUint64(bt[34:42], uint64(p.AccessExecAt))
	b.PutUint64(bt[42:50], p.Tag)
	b.PutUint16(bt[50:52], uint16(p.Priority))
	b.PutUint64(bt[52:60], uint64(p.offset))
	b.PutUint64(bt[60:68], uint64(p.dataLen))
	b.PutUint16(bt[68:70], itemEnd)

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

	p.Index = int64(b.Uint64(bt[2:10]))
	copy(p.Id[:], bt[10:26])
	p.CreatedAt = int64(b.Uint64(bt[26:34]))
	p.AccessExecAt = int64(b.Uint64(bt[34:42]))
	p.Tag = b.Uint64(bt[42:50])
	p.Priority = Priority(binary.LittleEndian.Uint16(bt[50:52]))
	p.offset = int64(b.Uint64(bt[52:60]))
	p.dataLen = int64(b.Uint64(bt[60:68]))
	if b.Uint16(bt[68:70]) != itemEnd {
		return 0, ErrDataFail
	}
	return n, err
}
