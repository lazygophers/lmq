package mq

import "errors"

var (
	ErrTopicAlreadyExist = errors.New("topic already exists")
	ErrTopicNoExist      = errors.New("topic does not exist")
)

type TopicManager struct {
	topicList []*Topic
}

func NewTopicManager() *TopicManager {
	return &TopicManager{}
}

func (p *TopicManager) getTopic(name string) (int, *Topic) {
	for i, t := range p.topicList {
		if t.name == name {
			return i, t
		}
	}
	return -1, nil
}

func (p *TopicManager) AddTopic(name string, config *Config) error {
	_, t := p.getTopic(name)
	if t != nil {
		return ErrTopicAlreadyExist
	}
	topic, err := NewTopic(name, config)
	if err != nil {
		return err
	}
	p.topicList = append(p.topicList, topic)
	return nil
}

func (p *TopicManager) RemoveTopic(name string) error {
	i, t := p.getTopic(name)
	if t == nil {
		return ErrTopicNoExist
	}
	p.topicList = append(p.topicList[:i], p.topicList[i+1:]...)
	return nil
}
