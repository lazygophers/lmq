package state

import (
	"github.com/lazygophers/lmq"
	"github.com/lazygophers/log"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"sync"
)

type QueueInterface interface {
	PubMessage(msg *lmq.Message) error
	Pop() *lmq.Message
}

type Queue struct {
	queue *DiskQueue

	inFlightLock     sync.RWMutex
	inFlightMessages map[string][]byte
}

func (p *Queue) PopChan() <-chan []byte {
	return p.queue.ReadChan()
}

func (p *Queue) PubMessage(msg *lmq.Message) error {
	if msg.MsgId == "" {
		msg.MsgId = GenMessageId()
	}

	if !msg.CreatedAt.IsValid() {
		msg.CreatedAt = timestamppb.Now()
	}

	buffer, err := proto.Marshal(msg)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	err = p.queue.Put(buffer)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	return nil
}

func (p *Queue) checkSkip(msg *lmq.Message) bool {
	return false
}

func (p *Queue) pop() *lmq.Message {
	for i := 0; i < 10; i++ {
		select {
		case x := <-p.queue.ReadChan():
			var msg lmq.Message
			err := proto.Unmarshal(x, &msg)
			if err != nil {
				log.Errorf("err:%v", err)
				return nil
			}

			if p.checkSkip(&msg) {
				continue
			}

			p.inFlightLock.Lock()
			p.inFlightMessages[msg.MsgId] = x
			p.inFlightLock.Unlock()

			return &msg

		default:
		}
	}

	return nil
}

func (p *Queue) Pop() *lmq.Message {
	msg := p.pop()
	if msg == nil {
		return nil
	}

	return msg
}

func (p *Queue) Close() {
	p.inFlightLock.Lock()
	for key, buffer := range p.inFlightMessages {
		_ = p.queue.Put(buffer)
		delete(p.inFlightMessages, key)
	}
	p.inFlightLock.Unlock()

	_ = p.queue.Close()
}

func (p *Queue) Remove() {
	p.inFlightLock.Lock()
	p.inFlightMessages = make(map[string][]byte)
	p.inFlightLock.Unlock()

	_ = p.queue.Empty()
	_ = p.queue.Delete()
}

func (p *Queue) Depth() (depth int64) {
	depth += p.queue.Depth()

	p.inFlightLock.RLock()
	depth += int64(len(p.inFlightMessages))
	p.inFlightLock.RUnlock()

	return depth
}

func (p *Queue) Finish(msgId string) {
	p.inFlightLock.Lock()
	delete(p.inFlightMessages, msgId)
	p.inFlightLock.Unlock()
}

func (p *Queue) GetInFlight(msgId string) *lmq.Message {
	p.inFlightLock.RLock()
	defer p.inFlightLock.RUnlock()

	if data, ok := p.inFlightMessages[msgId]; ok {
		var msg lmq.Message
		err := proto.Unmarshal(data, &msg)
		if err != nil {
			log.Errorf("err:%v", err)
			return nil
		}

		return &msg
	}

	return nil
}

func (p *Queue) Requeue(msg *lmq.Message) error {
	err := p.PubMessage(msg)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	p.Finish(msg.MsgId)

	return nil
}
