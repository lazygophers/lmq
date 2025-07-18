package mq

import (
	"errors"
	"github.com/lazygophers/log"
	"sync"
	"time"
)

var ErrChannelAlreadyExists = errors.New("channel already exists")

type Topic struct {
	name  string
	queue *Queue

	channelMux sync.RWMutex
	channelMap map[string]*Channel
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
	ticker := time.NewTicker(2 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			err := p.pull()
			if err != nil {
				log.Errorf("pull err:%s", err)
			}
		case <-p.queue.topicPullChan:
			err := p.pull()
			if err != nil {
				log.Errorf("pull err:%s", err)
			}
		}
	}
}

func (p *Topic) GetOrAddChannel(name string, config *Config) (*Channel, error) {
	p.channelMux.RLock()
	channel := p.channelMap[name]
	p.channelMux.RUnlock()

	if channel != nil {
		return channel, ErrChannelAlreadyExists
	}

	p.channelMux.Lock()
	defer p.channelMux.Unlock()

	channel = p.channelMap[name]
	if channel != nil {
		return channel, ErrChannelAlreadyExists
	}

	channel, err := NewChannel(name, config)
	if err != nil {
		log.Errorf("NewChannel err:%s", err.Error())
		return nil, err
	}

	p.channelMap[name] = channel
	return channel, nil
}

func (p *Topic) RemoveChannel(name string) error {
	err := p.channelMap[name].Close()
	if err != nil {
		return err
	}

	delete(p.channelMap, name)
	return nil
}

func (p *Topic) UpdateChannel(name string, config *Config) error {
	c, err := p.GetOrAddChannel(name, config)
	if c == nil {
		return err
	}

	p.channelMux.Lock()
	c.queue.config = config
	p.channelMux.Unlock()
	return nil
}

func (p *Topic) Push(msg *Message) error {
	err := p.queue.Write(msg)
	if err != nil {
		return err
	}

	p.queue.topicPullChan <- msg

	return nil
}

/*func (p *Topic) Pull() {
	ticker := time.NewTicker(2 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		if p.queue.readerPart < p.queue.writerPart && p.queue.readSize == 0 || p.queue.readerPart == p.queue.writerPart && p.queue.readSize < p.queue.writeSize {
			p.queue.topicPullChan <- true
		}
	}
}*/

func (p *Topic) pull() error {
	// 退出条件
	if p.queue.readerPart < p.queue.writerPart && p.queue.readSize != 0 || p.queue.readerPart == p.queue.writerPart && p.queue.readSize < p.queue.writeSize {
		return nil
	}
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
	for _, v := range p.channelMap {
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
