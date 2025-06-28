package mq

import (
	"bytes"
	"testing"
	"time"
)

func TestMsg(t *testing.T) {
	m := &Message{
		Id:        GenMessageId(),
		CreatedAt: time.Now().Unix(),
	}

	var b bytes.Buffer
	_, err := m.WriteTo(&b)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(b.Len())
	_, err = m.ReadFrom(&b)
	if err != nil {
		t.Fatal(err)
	}

	t.Log(m)
}
