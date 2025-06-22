package state

import (
	"github.com/lazygophers/lmq"
	"github.com/lazygophers/log"
	"github.com/lazygophers/utils/candy"
	"github.com/lazygophers/utils/routine"
	"github.com/lazygophers/utils/unit"
	"google.golang.org/protobuf/proto"
	"maps"
	"sync"
)

type Topic struct {
	manage *TopicManage

	sync.RWMutex
	*Queue

	c *lmq.Topic

	cm map[string]*Channel
}

func (p *Topic) Topic() *lmq.Topic {
	p.RLock()
	defer p.RUnlock()

	return p.c
}

func (p *Topic) RealDepth() (depth int64) {
	p.RLock()
	defer p.RUnlock()

	for _, c := range p.cm {
		depth += c.Depth()
	}

	depth += p.Depth()

	return p.Depth()
}

func (p *Topic) setTopic(topic *lmq.Topic) error {
	p.Lock()
	p.c = topic
	p.Unlock()

	return nil
}

func (p *Topic) Remove() {
	p.Lock()
	defer p.Unlock()

	for _, channel := range p.cm {
		channel.Remove()
	}
}

func (p *Topic) setChannel(c *lmq.Channel) (err error) {
	p.RLock()
	t := p.cm[c.Name]
	p.RUnlock()

	if t == nil {
		err := func() error {
			p.Lock()
			defer p.Unlock()

			t = p.cm[c.Name]
			if t != nil {
				return nil
			}

			t, err = p.NewChannel(c)
			if err != nil {
				log.Errorf("err:%v", err)
				return err
			}

			p.cm[c.Name] = t

			return nil
		}()
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
	}

	err = t.setChannel(c)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	return nil
}

func (p *Topic) delChannel(name string) error {
	channel := p.getChannel(name)
	if channel == nil {
		return nil
	}

	channel.Remove()

	return nil
}

func (p *Topic) getChannel(name string) *Channel {
	p.RLock()
	defer p.RUnlock()

	return p.cm[name]
}

func (p *Topic) GetChannel(name string) *Channel {
	p.RLock()
	defer p.RUnlock()

	return p.cm[name]
}

func (p *Topic) DelChannel(name string) error {
	topic := p.Topic()

	topic.ChannelList = candy.FilterNot(topic.ChannelList, func(channel *lmq.Channel) bool {
		return channel.Name == name
	})

	err := p.manage.config.SetPb(p.manage.getTopicConfigKey(topic.Name), topic)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	return nil
}

func (p *Topic) SetChannel(channel *lmq.Channel) error {
	topic := p.Topic()
	topic.ChannelList = append(topic.ChannelList, channel)

	topic.ChannelList = candy.UniqueUsing(topic.ChannelList, func(channel *lmq.Channel) any {
		return channel.Name
	})

	err := p.manage.config.SetPb(p.manage.getTopicConfigKey(topic.Name), topic)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	return nil
}

func (p *Topic) getOrCreateChannel(c *lmq.Channel) (*Channel, error) {
	p.RLock()
	t := p.cm[c.Name]
	p.RUnlock()

	if t != nil {
		return t, nil
	}

	p.Lock()
	defer p.Unlock()

	t = p.cm[c.Name]
	if t != nil {
		return t, nil
	}

	t, err := p.NewChannel(c)
	if err != nil {
		return nil, err
	}

	p.cm[c.Name] = t

	return t, nil
}

func (p *Topic) ChannelList() []*Channel {
	p.RLock()
	defer p.RUnlock()

	channels := make([]*Channel, 0, len(p.cm))
	for _, channel := range p.cm {
		channels = append(channels, channel)
	}

	return channels
}

func (p *Topic) init() error {
	p.RLock()
	cc := p.c.ChannelList
	cm := maps.Clone(p.cm)
	p.RUnlock()

	for _, channel := range cc {
		delete(cm, channel.Name)
		_, err := p.getOrCreateChannel(channel)
		if err != nil {
			return err
		}
	}

	// 移除不存在的
	for _, channel := range cm {
		channel.Remove()
	}

	return nil
}

func (p *TopicManage) NewTopic(c *lmq.Topic) (*Topic, error) {
	pp := &Topic{
		manage: p,

		c:  c,
		cm: make(map[string]*Channel, len(c.ChannelList)),
		Queue: &Queue{
			inFlightMessages: make(map[string][]byte),
		},
	}

	if pp.c.DiskQueue == nil {
		pp.c.DiskQueue = &lmq.DiskQueue{}
	}
	if pp.c.DiskQueue.MaxFilePartSize == 0 {
		pp.c.DiskQueue.MaxFilePartSize = unit.MB * 10
	}

	pp.queue = NewDiskQueue(c.Name, int32(c.MaxMsgSize))

	routine.GoWithMustSuccess(func() (err error) {
		for body := range pp.PopChan() {
			var msg lmq.Message
			err = proto.Unmarshal(body, &msg)
			if err != nil {
				log.Errorf("err:%v", err)
				continue
			}

			candy.Each(pp.ChannelList(), func(channel *Channel) {
				err = channel.PubMessage(proto.Clone(&msg).(*lmq.Message))
				if err != nil {
					log.Errorf("err:%v", err)
					return
				}
			})
		}

		return nil
	})

	return pp, nil
}
