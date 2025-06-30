package mq

type Channel struct {
	name  string
	queue *Queue
}

/*func NewChannel(name string, path string, maxSize int) (*Channel, error {
	queue, err := NewQueue(name, NewConfig(path, maxSize))
	if err != nil {
		return nil, err
	}
	return &Channel{
		name: name,
		queue: queue,
	},  nil

}

func (p *Channel) Pull() {

}

func (p *Channel) Push() {

}

func (p *Channel) getMessage() {

}*/
