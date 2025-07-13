package mq

import (
	"github.com/lazygophers/log"
	"strconv"
)

type Topic struct {
	name     string
	queue    *Queue
	chanlist []*Channel // TODO: NAME 的值要唯一
}

func NewTopic(name string, queue *Queue) *Topic {
	return &Topic{
		name:  name,
		queue: queue,
	}
}

// TODO: get  or add
func (p *Topic) AddChannel(queue *Queue) {
	c := NewChannel(p.name+strconv.Itoa(len(p.chanlist)), queue)

	p.chanlist = append(p.chanlist, c)
}

func (p *Topic) RemoveChannel(i int) error {
	err := p.chanlist[i].Close()
	if err != nil {
		return err
	}

	p.chanlist = append(p.chanlist[:i], p.chanlist[i+1:]...)
	return nil
}

func (p *Topic) Push(msg *Message) error {
	err := p.queue.Write(msg)
	if err != nil {
		return err
	}

	err = p.pull()
	if err != nil {
		return err
	}
	return nil
}

// TODO: 数据的copy应该是自动化的，非用户主动触发
func (p *Topic) pull() error {
	// todo: 尝试刷新一下reader的信息以及reader buffer

	readerBuffer := make([]*Message, len(p.queue.readerBuffer))
	copy(readerBuffer, p.queue.readerBuffer[:len(readerBuffer)])

	for _, v := range p.chanlist {
		err := p.queue.transferMsg(v.queue, p.queue, readerBuffer)
		if err != nil {
			log.Errorf("err:%s", err)
			return err
		}
	}

	// 标记 readerBuffer 的数据都done了
	// 移除  readerBuffer 的部分在 p.readerBuffer 内

	return nil
}
