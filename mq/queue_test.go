package mq

import (
	"github.com/lazygophers/utils/randx"
	"os"
	"sync"
	"testing"
	"time"
)

type DataType struct{}

func Test(t *testing.T) {
	err := os.RemoveAll("E:\\GH\\lmq\\mq\\test")
	if err != nil {
		t.Fatal(err)
		return
	}

	name := "test"
	queue, err := NewQueue(name,
		NewConfig(
			"E:\\GH\\lmq\\mq\\"+name,
			100,
			3,
			30,
			randx.Choose([]retryType{fixed, linear, exponential})))
	if err != nil {
		t.Fatal(err)
		return
	}

	var w sync.WaitGroup
	for i := 0; i < 50; i++ {
		w.Add(1)
		go func() {
			defer w.Done()
			err := queue.Write(&Message{
				Id:        GenMessageId(),
				CreatedAt: time.Now().Unix() + int64(i),
				Data:      []byte("hee"),
				//AccessExecAt: time.Now().Unix() + randx.Int64(),
				Hash:     uint64(i),
				Priority: randx.Choose([]Priority{PriorityLow, PriorityMiddle, PriorityHigh}),
			})
			if err != nil {
				t.Error(err)
			}
		}()
	}

	w.Wait()

	ch := &Channel{
		name:  "test",
		queue: queue,
	}
	t.Log(ch)

	w.Add(50)

	ch.Process(3, func(msg *Message, data *DataType) (resp *ConsumeResp, err error) {
		defer w.Done()

		resp = &ConsumeResp{
			needSkip:  false,
			needRetry: false,
			baseDelay: time.Microsecond,
		}
		t.Log(resp)
		return resp, nil
	})

	w.Wait()

	/*for i := 0; i < 50; i++ {
		t.Log(i)
		m := queue.Read()
		if m == nil {
			continue
		}

		t.Logf("Read msg No.%d :%v", i, m)

		err := queue.Done(&doneRq{
			msgId:     m.Id,
			needRetry: randx.Booln(0),
			needSkip:  randx.Choose([]bool{true, false}),
			baseDelay: time.Microsecond,
			errChan:   make(chan error, 1),
		})
		if err != nil {
			t.Error(err)
		}
	}*/
}

func TestXXX(t *testing.T) {

}

func BenchmarkXXX(b *testing.B) {
	// NewQueue
	b.Cleanup()
	for i := 0; i < b.N; i++ {
		b.Log("1+1")
	}
}
