package mq

import (
	"github.com/lazygophers/log"
	"github.com/lazygophers/utils/json"
	"github.com/lazygophers/utils/runtime"
	"reflect"
	"time"
)

type ConsumeResp struct {
	needSkip  bool
	needRetry bool
	baseDelay time.Duration
}

type Channel struct {
	name  string
	queue *Queue
}

func NewChannel(name string, config *Config) (*Channel, error) {
	queue, err := NewQueue(name, config)
	if err != nil {
		log.Errorf("NewChannel err:%s", err.Error())
		return nil, err
	}
	return &Channel{
		name:  name,
		queue: queue,
	}, nil
}

// Consumer要数据
func (p *Channel) Pull() *Message {
	return p.queue.Read()
}

// Topic给数据
func (p *Channel) Push(msg *Message) error {
	return p.queue.Write(msg)
}

// Consumer消费数据
func (p *Channel) Process(consumeCnt int, logic any) {
	if consumeCnt <= 0 {
		consumeCnt = 1
	}

	v := reflect.ValueOf(logic)
	if v.Kind() != reflect.Func {
		panic("not a function")
		return
	}

	t := v.Type()
	numIn := t.NumIn()
	if numIn < 2 {
		panic("The function args num < 2.")
		return
	}

	// 校验入参类型
	in1 := t.In(0)
	for in1.Kind() == reflect.Ptr {
		in1 = in1.Elem()
	}
	if in1.Kind() != reflect.Struct {
		panic("The function in the first args is not a struct.")
		return
	}
	if in1.PkgPath() != "github.com/lazygophers/lmq/mq" {
		log.Debug(in1.PkgPath())
		panic("The pkg of function in the first args is incorrect.")
		return
	}

	in2 := t.In(1)
	for in2.Kind() == reflect.Ptr {
		in2 = in2.Elem()
	}
	if in2.Kind() != reflect.Struct {
		panic("The function in the second args is not a struct.")
		return
	}

	numOut := t.NumOut()
	if numOut < 2 {
		panic("The function out args num < 2.")
		return
	}

	//校验出参类型
	out1 := t.Out(0)
	for out1.Kind() == reflect.Ptr {
		out1 = out1.Elem()
	}
	if out1.Kind() != reflect.Struct {
		panic("The function in the first args is not a struct.")
		return
	}
	if out1.PkgPath() != "github.com/lazygophers/lmq/mq" {
		panic("The pkg of function in the first args is incorrect.")
		return
	}

	out2 := t.Out(1)
	for out2.Kind() == reflect.Ptr {
		out2 = out2.Elem()
	}
	if out2.Name() != "error" {
		panic("The function in the second args is not a error.")
		return
	}

	handle := func(msg *Message) (*ConsumeResp, error) {
		defer runtime.CachePanicWithHandle(func(err interface{}) {

		})
		inReq := reflect.New(in2)

		log.Debugf("inReq:%#v", inReq.Interface())
		err := json.Unmarshal(msg.Data, inReq.Interface())
		if err != nil {
			log.Errorf("err:%s", err)
			return nil, err
		}

		log.Debugf("in1 value:%#v", reflect.ValueOf(msg))
		log.Debugf("in2 value:%#v", inReq)
		out := reflect.ValueOf(logic).Call([]reflect.Value{reflect.ValueOf(msg), inReq})
		return out[0].Interface().(*ConsumeResp), out[1].Interface().(error)

	}

	for i := 0; i < consumeCnt; i++ {
		go func() {
			for m := range p.queue.Pull() {
				log.Debugf("receive msg:%#v", m)

				/*if m == nil {
					//time.Sleep(time.Microsecond * 200)
					return
				}*/

				resp, err := handle(m)
				if err != nil {
					log.Errorf("err:%s", err)
				}
				dq := &doneRq{
					msgId:     m.Id,
					needSkip:  false,
					needRetry: false,
					baseDelay: 0,
					errChan:   make(chan error, 1),
				}
				if resp != nil {
					dq.needSkip = resp.needSkip
					dq.needRetry = resp.needRetry
					dq.baseDelay = resp.baseDelay
				}
				err = p.queue.Done(dq)
				if err != nil {
					log.Errorf("err:%s", err)
					continue
				}
			}
		}()
	}
}

func (p *Channel) Close() error {
	return p.queue.Close()
}
