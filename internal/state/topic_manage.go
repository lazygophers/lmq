package state

import (
	"fmt"
	"github.com/lazygophers/lmq"
	"github.com/lazygophers/lrpc/middleware/config"
	"github.com/lazygophers/lrpc/middleware/core"
	"github.com/lazygophers/lrpc/middleware/xerror"
	"github.com/lazygophers/utils/atexit"
	"github.com/lazygophers/utils/candy"
	"github.com/prometheus/common/log"
	"google.golang.org/protobuf/proto"
	"strings"
	"sync"
)

type TopicManage struct {
	sync.RWMutex

	m map[string]*Topic

	config *config.Config
}

func (p *TopicManage) GetTopic(name string) *Topic {
	p.RLock()
	defer p.RUnlock()

	return p.m[name]
}

func (p *TopicManage) getOrCreateTopic(name string) (*Topic, error) {
	p.RLock()
	t := p.m[name]
	p.RUnlock()

	if t != nil {
		return t, nil
	}

	p.Lock()
	defer p.Unlock()

	t = p.m[name]
	if t != nil {
		return t, nil
	}

	var topic lmq.Topic
	err := p.config.GetPb(name, &topic)
	if err != nil {
		if xerror.CheckCode(err, int32(core.ErrCode_ConfigNotFound)) {
			return nil, nil
		}

		log.Errorf("err:%v", err)
		return nil, err
	}

	t, err = p.NewTopic(&topic)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	p.m[name] = t

	return t, nil
}

func (*TopicManage) getTopicConfigKey(name string) string {
	return fmt.Sprintf("%s%s", configTopicPrefix, name)
}

const (
	configTopicPrefix = "topic_"
)

func (p *TopicManage) RestoreTopics() (err error) {
	keys, err := p.config.ListPrefix(configTopicPrefix)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	log.Infof("got %d topics", len(keys))

	for _, key := range keys {
		log.Infof("get topic key %s", key)

		key = strings.TrimPrefix(key, configTopicPrefix)

		_, err = p.getOrCreateTopic(key)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
	}

	log.Infof("restore topic success")

	return nil
}

func (p *TopicManage) setTopic(c *lmq.Topic) (*Topic, error) {
	var err error
	name := c.Name

	p.RLock()
	t := p.m[name]
	p.RUnlock()
	if t == nil {
		err = func() error {
			p.Lock()
			defer p.Unlock()

			t = p.m[name]

			if t != nil {
				return nil
			}

			t, err = p.NewTopic(c)
			if err != nil {
				log.Errorf("err:%v", err)
				return err
			}

			p.m[name] = t

			return nil
		}()
		if err != nil {
			log.Errorf("err:%v", err)
			return nil, err
		}
	}

	err = t.setTopic(c)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	err = t.init()
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	return t, nil
}

func (p *TopicManage) delTopic(name string) error {
	p.RLock()
	t := p.m[name]
	p.RUnlock()

	if t == nil {
		return nil
	}

	t.Remove()

	p.Lock()
	defer p.Unlock()

	delete(p.m, name)

	return nil
}

func (p *TopicManage) SetTopic(topic *lmq.Topic) error {
	old := p.GetTopic(topic.Name)
	if old != nil {
		for _, channel := range old.Topic().ChannelList {
			topic.ChannelList = append(topic.ChannelList, channel)
		}
	}

	topic.ChannelList = candy.UniqueUsing(topic.ChannelList, func(channel *lmq.Channel) any {
		return channel.Name
	})

	err := p.config.SetPb(topic.Name, topic)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	return nil
}

func (p *TopicManage) TopicList() []*Topic {
	p.RLock()
	defer p.RUnlock()

	topics := make([]*Topic, 0, len(p.m))
	for _, topic := range p.m {
		topics = append(topics, topic)
	}

	return topics
}

func (p *TopicManage) Close() {
	p.Lock()
	defer p.Unlock()

	for _, topic := range p.m {
		topic.Close()
	}
}

func NewTopicManage() (*TopicManage, error) {
	p := &TopicManage{
		config: config.NewConfig(State.Etcd, "lmq"),
		m:      make(map[string]*Topic),
	}

	atexit.Register(func() {
		p.Close()
	})

	p.config.OnChanged(func(item *core.ConfigItem, eventType config.EventType) {
		switch eventType {
		case config.Changed:
			var topic lmq.Topic
			err := proto.Unmarshal(item.Value, &topic)
			if err != nil {
				log.Errorf("err:%v", err)
				return
			}

			_, err = p.setTopic(&topic)
			if err != nil {
				log.Errorf("err:%v", err)
				return
			}

		case config.Deleted:
			err := p.delTopic(strings.TrimPrefix(item.Key, configTopicPrefix))
			if err != nil {
				log.Errorf("err:%v", err)
				return
			}
		}
	})

	err := p.RestoreTopics()
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	return p, nil
}
