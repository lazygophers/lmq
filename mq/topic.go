package mq

import (
	"errors"
	"github.com/lazygophers/log"
	"strconv"
	"sync"
)

var ErrChannelAlreadyExists = errors.New("channel already exists")

type Topic struct {
	name     string
	queue    *Queue
	chanlist []*Channel // TODO: NAME 的值要唯一
}

func NewTopic(name string, config *Config) (*Topic, error) {
	queue, err := NewQueue(name, config)
	if err != nil {
		log.Errorf("NewTopic err:%s", err.Error())
		return nil, err
	}

	p := &Topic{
		name:  name,
		queue: queue,
	}

	go p.loop()

	return p, nil
}

func (p *Topic) loop() {
	for {
		// 新文件切片启动pull
		if p.queue.readerPart < p.queue.writerPart && p.queue.readSize == 0 {
			err := p.pull()
			if err != nil {
				log.Errorf("pull err:%s", err)
			}
		}

		// 新写入启动pull
		if p.queue.readerPart == p.queue.writerPart && p.queue.readSize < p.queue.writeSize {
			err := p.pull()
			if err != nil {
				log.Errorf("pull err:%s", err)
			}
		}
	}
}

func (p *Topic) AddChannel(config *Config) error {
	if p.chanlist[len(p.chanlist)] != nil {
		log.Errorf("channel already exists")
		return ErrChannelAlreadyExists
	}

	c, err := NewChannel(p.name+strconv.Itoa(len(p.chanlist)), config)
	if err != nil {
		log.Errorf("AddChannel err:%s", err.Error())
		return err
	}

	p.chanlist = append(p.chanlist, c)
	return nil
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

	return nil
}

func (p *Topic) Pull() error {

	return nil
}

func (p *Topic) pull() error {
	// todo: 尝试刷新一下reader的信息以及reader buffer
	//if p.queue.readerPart == p.queue.writerPart {
	err := p.queue.readIdx()
	if err != nil {
		return err
	}
	//}

	size := len(p.queue.readerBuffer)
	readerBuffer := make([]*Message, size)
	copy(readerBuffer, p.queue.readerBuffer[:size])

	var w *sync.WaitGroup
	for _, v := range p.chanlist {
		w.Add(1)
		go func() {
			defer w.Done()
			// 失败重试
			for {
				err := p.queue.transferMsg(v.queue, readerBuffer)
				if err == nil {
					break
				}
				log.Errorf("err:%s", err)
			}
		}()
	}
	w.Wait()

	// 标记 readerBuffer 的数据都done了
	for _, msg := range readerBuffer {
		err := p.queue.writeDone(msg.Id)
		if err != nil {
			return err
		}
	}

	//更新readerBuffer
	p.queue.readerBuffer = p.queue.readerBuffer[size:]

	return nil
}
