package mq

import (
	"github.com/lazygophers/utils/randx"
	"os"
	"testing"
	"time"
)

func TestWR(t *testing.T) {
	err := os.RemoveAll("E:\\GH\\lmq\\mq\\test")
	if err != nil {
		t.Fatal(err)
		return
	}
	name := "test"
	queue, err := NewQueue(name, &Config{
		path:    "E:\\GH\\lmq\\mq\\" + name,
		maxSize: 1024,
	})
	t.Error(err)

	var ms []*Message
	for i := 0; i < 10; i++ {
		ms = append(ms, &Message{
			Id:        GenMessageId(),
			CreatedAt: time.Now().Unix() + int64(i),
			Data:      []byte("hee"),
			//AccessExecAt: time.Now().Unix() + randx.Int64(),
			Tag:      uint64(i),
			Priority: randx.Choose([]Priority{PriorityLow, PriorityMiddle, PriorityHigh}),
		})
	}

	for _, m := range ms {
		queue.Write(m)

	}

	for i := 0; i < 10; i++ {
		t.Log(i)

		t.Logf("读出的第%d个msg:%v", i, queue.Read())
	}

}
