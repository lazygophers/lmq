package state

import (
	"fmt"
	"github.com/lazygophers/lmq"
	"google.golang.org/protobuf/types/known/timestamppb"
	"sync"
	"time"
)

type Channel struct {
	sync.RWMutex

	*Queue

	c *lmq.Channel

	topic string
}

func (p *Channel) setChannel(channel *lmq.Channel) error {
	p.Lock()
	defer p.Unlock()

	p.c = channel

	return nil
}

func (p *Channel) Channel() *lmq.Channel {
	p.RLock()
	defer p.RUnlock()

	return p.c
}

func (p *Channel) PubMessage(msg *lmq.Message) error {
	p.coverMsg(msg)

	return p.Queue.PubMessage(msg)
}

func (p *Channel) Requeue(msg *lmq.Message) error {
	p.coverMsg(msg)

	return p.Queue.Requeue(msg)
}

func (p *Channel) coverMsg(msg *lmq.Message) {
	c := p.Channel()

	if c.ExpiryTime.IsValid() && !msg.ExpireAt.IsValid() {
		msg.ExecAt = timestamppb.New(time.Now().Add(c.ExpiryTime.AsDuration()))
	}

	if c.DelayTime.IsValid() && !msg.ExecAt.IsValid() {
		msg.ExecAt = timestamppb.New(time.Now().Add(c.DelayTime.AsDuration()))
	}
}

func (p *Topic) NewChannel(c *lmq.Channel) (*Channel, error) {
	pp := &Channel{
		c:     c,
		topic: p.c.Name,
		Queue: &Queue{
			inFlightMessages: make(map[string][]byte),
		},
	}

	// 先用 nsq 的 diskqueue 做一下，后面再慢慢前一
	pp.queue = NewDiskQueue(fmt.Sprintf("%s.%s", pp.topic, pp.c.Name), int32(p.c.MaxMsgSize))

	return pp, nil
}
