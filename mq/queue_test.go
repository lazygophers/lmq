package mq

import (
	"testing"
	"time"
)

func TestWR(t *testing.T) {
	name := "test"
	queue := NewQueue(name, &Config{
		path:       "E:\\GH\\lmq\\mq\\" + name,
		maxSize:    1024,
		enableDisk: true,
	})

	for i := 0; i < 10; i++ {
		ms := &Message{
			Id:        GenMessageId(),
			CreatedAt: time.Now().Unix() + int64(i),
			Data:      []byte("hee"),
		}
		wq := &writerReq{
			message: ms,
			errChan: make(chan error, 1),
		}

		queue.Write(wq)

	}

	for i := 0; i < 10; i++ {
		rq := make(chan *Message)
		queue.Read(rq)

		msg := <-rq
		t.Logf("读出的第%d个msg:%v", i, msg)
	}

}
