package mq

import (
	"errors"
	"github.com/lazygophers/log"
	"github.com/lazygophers/utils/anyx"
	"github.com/lazygophers/utils/candy"
	"github.com/lazygophers/utils/osx"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const readDataNum = 10
const maxRunningNum = 3

var ERRIDXNULL = errors.New("idx is null")

type writerReq struct {
	message *Message
	errChan chan error
}

// consumer 写
type doneRq struct {
	msgId   MessageId
	errChan chan error
}

type Config struct {
	path    string
	maxSize int
}

func NewConfig(path string, maxSize int) *Config {
	return &Config{
		path:    path,
		maxSize: maxSize,
	}
}

/*
文件三部分：
.dat: message的buffer
.idx: 索引
.done: 完成的消息id
*/
type Queue struct {
	name   string
	config *Config

	writerChan chan *writerReq
	datWriter  *os.File
	idxWriter  *os.File
	doneWriter *os.File
	writerPart uint64 //writer文件游标
	writeSize  int

	readerBuffer      []*Message
	runningBufferMap  map[uint64]int64
	runningMessageMap map[MessageId]*Message
	readerDone        map[MessageId]bool
	readerChan        chan chan *Message
	datReader         *os.File
	idxReader         *os.File
	doneReader        *os.File
	doneChan          chan *doneRq
	readerPart        uint64 //reader文件游标
	readSize          int
}

func NewQueue(name string, config *Config) (*Queue, error) {
	p := &Queue{
		name:              name,
		config:            config,
		writerChan:        make(chan *writerReq),
		readerChan:        make(chan chan *Message),
		readerBuffer:      make([]*Message, 0),
		runningBufferMap:  make(map[uint64]int64),
		runningMessageMap: make(map[MessageId]*Message),
		readerDone:        make(map[MessageId]bool),
		readerPart:        1,
		writerPart:        1,
	}

	if !osx.IsDir(config.path) {
		err := os.MkdirAll(config.path, os.ModePerm)
		if err != nil {
			log.Errorf("err:%s", err)
			return nil, err
		}
	}
	//读文件列表
	partList, err := os.ReadDir(config.path)
	if err != nil {
		log.Errorf("error: %s", err)
		return nil, err
	}

	for i, part := range partList {
		name := part.Name()
		name = strings.TrimSuffix(filepath.Base(name), filepath.Ext(name))
		n := anyx.ToUint64(name)

		if i == 0 {
			p.readerPart = n
		}

		if n > 0 && n < p.readerPart {
			p.readerPart = n
		}
		if n > p.writerPart {
			p.writerPart = n
		}
	}

	//加载写文件
	err = p.openWriter()
	if err != nil {
		log.Errorf("err:%s", err)
		return nil, err
	}

	//加载读文件
	err = p.openReader()
	if err != nil {
		log.Errorf("err:%s", err)
		return nil, err
	}

	_ = p.preRead()
	go p.loop()

	return p, nil
}

func (p *Queue) openReader() (err error) {
	if p.datReader != nil {
		_ = p.datWriter.Close()
		log.Debug("file close")
	}
	p.datReader, err = os.OpenFile(filepath.Join(p.config.path, anyx.ToString(p.readerPart)+".dat"), os.O_RDONLY, 0600)
	if err != nil {
		log.Errorf("err:%s", err)
		return err
	}

	if p.idxReader != nil {
		_ = p.idxReader.Close()
		log.Debug("file close")
	}
	p.idxReader, err = os.OpenFile(filepath.Join(p.config.path, anyx.ToString(p.readerPart)+".idx"), os.O_RDONLY, 0600)
	if err != nil {
		log.Errorf("err:%s", err)
		return err
	}

	if p.doneReader != nil {
		_ = p.doneReader.Close()
		log.Debug("file close")
	}
	p.doneReader, err = os.OpenFile(filepath.Join(p.config.path, anyx.ToString(p.readerPart)+".done"), os.O_RDWR, 0600)
	if err != nil {
		log.Errorf("err:%s", err)
		return err
	}

	return nil
}

func (p *Queue) openWriter() (err error) {
	if p.datWriter != nil {
		_ = p.datWriter.Close()
		log.Debug("file close")
		p.datWriter = nil
	}
	p.datWriter, err = os.OpenFile(filepath.Join(p.config.path, anyx.ToString(p.writerPart)+".dat"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		log.Errorf("err:%s", err)
		return err
	}

	if p.idxWriter != nil {
		_ = p.idxWriter.Close()
		log.Debug("file close")
		p.idxWriter = nil
	}
	p.idxWriter, err = os.OpenFile(filepath.Join(p.config.path, anyx.ToString(p.writerPart)+".idx"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		_ = p.datWriter.Close()
		log.Debug("file close")
		p.datWriter = nil
		log.Errorf("err:%s", err)
		return err
	}

	if p.doneWriter != nil {
		_ = p.doneWriter.Close()
		log.Debug("file close")
		p.doneWriter = nil
	}
	p.doneWriter, err = os.OpenFile(filepath.Join(p.config.path, anyx.ToString(p.writerPart)+".done"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		_ = p.datWriter.Close()
		_ = p.idxWriter.Close()
		log.Debug("file close")
		p.datWriter = nil
		p.idxWriter = nil
		log.Errorf("err:%s", err)
		return err
	}

	size, _ := p.datWriter.Seek(0, 2)
	p.writeSize = int(size)

	return nil
}

func (p *Queue) Write(msg *Message) error {
	errChan := make(chan error, 1)
	p.writerChan <- &writerReq{
		message: msg,
		errChan: errChan,
	}
	err := <-errChan
	log.Infof("write msg:%v", msg)
	return err
}

func (p *Queue) Read() *Message {
	msgChan := make(chan *Message, 1)
	p.readerChan <- msgChan

	return <-msgChan
}

func (p *Queue) loop() {
	for {
		select {
		case wr := <-p.writerChan:
			//写文件切片启动
			msgs := []*Message{wr.message}
			errChans := []chan error{wr.errChan}
			log.Infof("write:%v", wr.message)
			for {
				select {
				case wr = <-p.writerChan:
					msgs = append(msgs, wr.message)
					errChans = append(errChans, wr.errChan)
					log.Infof("write:%v", wr.message)
				default:
					goto ENDW
				}
			}
		ENDW:
			err := p.writeTo(msgs)
			for _, ch := range errChans {
				ch <- err
			}
		case dn := <-p.doneChan:
			//写doneReader启动
			msgIds := []MessageId{dn.msgId}
			errChans := []chan error{dn.errChan}
			log.Infof("done:%v", dn.msgId)
			for {
				select {
				case dn = <-p.doneChan:
					msgIds = append(msgIds, dn.msgId)
					errChans = []chan error{dn.errChan}
					log.Infof("done:%v", dn.msgId)
				default:
					goto ENDWD
				}
			}
		ENDWD:
			err := p.writeDone(msgIds)
			for _, ch := range errChans {
				ch <- err
			}
		case rq := <-p.readerChan:
			p.readToChan(rq)
		}
	}

}

func (p *Queue) readToChan(msgChan chan *Message) {
	//读文件切片启动
	if len(p.readerBuffer) == 0 || len(p.readerBuffer[0].Data) == 0 {
		_ = p.preRead()
	}
	log.Infof("msg:%v", p.readerBuffer[0])
	for i, v := range p.readerBuffer {
		log.Infof("tag running num:%d", p.runningBufferMap[v.Tag])
		if p.runningBufferMap[v.Tag] >= maxRunningNum {
			log.Infof("> max running num")
			continue
		}

		if v.AccessExecAt > 0 && v.AccessExecAt > time.Now().Unix() {
			continue
		}

		_ = p.tryReadData(v)
		msgChan <- v
		p.runningBufferMap[v.Tag]++
		p.readerBuffer = candy.RemoveIndex(p.readerBuffer, i)
		log.Infof("readBuffer lenth:%v", len(p.readerBuffer))
		return
	}

	msgChan <- nil
}

func (p *Queue) writeTo(msgs []*Message) error {
	size := 0
	var bt []byte
	for _, msg := range msgs {
		size += len(msg.Data)
		bt = append(bt, msg.Data...)
	}
	//预估大小是否切换切片
	if p.needChangePart(size) {
		err := p.changePart()
		if err != nil {
			log.Errorf("err:%s", err)
			return err
		}
	}

	//写data
	_, err := p.datWriter.Write(bt)
	if err != nil {
		log.Errorf("err:%s", err)
		return err
	}

	//写index
	for _, msg := range msgs {
		msg.offset = int64(p.writeSize)
		msg.dataLen = int64(len(msg.Data))
		log.Infof("msg write to:%v", msg)
		_, err = msg.WriteTo(p.idxWriter)
		p.writeSize += len(msg.Data)
		if err != nil {
			return err
		}
	}

	if p.needChangePart(0) {
		err := p.changePart()
		if err != nil {
			log.Errorf("err:%s", err)
			return err
		}
	}

	return nil
}

// 预加载idx、对应dat
func (p *Queue) preRead() error {
	fileInfo, err := p.idxReader.Stat()
	if err != nil {
		log.Errorf("err:%s", err)
		return err
	}

	if fileInfo.Size() == 0 {
		log.Errorf("err:%s", ERRIDXNULL)
		return ERRIDXNULL
	}

	if fileInfo.Size() > int64(p.readSize) {
		err := p.readIdx()
		if err != nil {
			return err
		}
	}

	log.Debug("第一个:&v", p.readerBuffer[0])
	if len(p.readerBuffer[0].Data) == 0 {
		err := p.readDat()
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *Queue) tryReadData(msg *Message) error {
	if msg.dataLen == 0 {
		return nil
	}
	if len(msg.Data) > 0 {
		return nil
	}

	_, err := p.datReader.ReadAt(msg.Data, msg.offset)
	if err != nil {
		log.Errorf("err:%s", err)
		return err
	}
	return nil
}

func (p *Queue) readDat() error {
	for i := 0; i < readDataNum; i++ {
		msg := p.readerBuffer[i]

		if msg.dataLen != 0 {
			msg.Data = make([]byte, msg.dataLen)
			_, err := p.datReader.ReadAt(msg.Data, msg.offset)
			log.Debug("msg.data:", msg.Data)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *Queue) readIdx() error {
	fileInfo, err := p.idxReader.Stat()
	if err != nil {
		log.Errorf("err:%s", err)
		return err
	}

	var i int64
	flag := false
	for i < fileInfo.Size() {
		log.Debug("当前读指针的位置是：", p.readSize)
		msg := new(Message)
		//TODO: 读切换
		n, err := msg.ReadFrom(p.idxReader)
		if err != nil {
			log.Errorf("err:%s", err)
			return err
		}

		if !p.readerDone[msg.Id] {
			p.readerBuffer = append(p.readerBuffer, msg)
			flag = true
			log.Infof("idx读出的msg:%v", msg)
		}

		i += int64(n)

		p.readSize = int(i)
	}

	if flag {
		p.readerBuffer = candy.SortUsing(p.readerBuffer, func(a, b *Message) bool {
			return a.Priority > b.Priority
		})
	}

	return nil
}

func (p *Queue) readDone() error {
	fileInfo, err := p.doneReader.Stat()
	if err != nil {
		log.Errorf("err:%s", err)
		return err
	}

	var i int64
	for i < fileInfo.Size() {
		var doneMsgId MessageId
		n, err := p.doneReader.Read(doneMsgId[:])
		if err != nil {
			log.Errorf("err:%s", err)
			return err
		}

		p.readerDone[doneMsgId] = true

		i += int64(n)
	}

	return nil
}

func (p *Queue) writeDone(msgIds []MessageId) error {
	for _, msgId := range msgIds {
		_, err := p.doneReader.Write(msgId[:])
		if err != nil {
			return err
		}

		p.readerDone[msgId] = true

		tag := p.runningMessageMap[msgId].Tag
		if p.runningBufferMap[tag] > 1 {
			p.runningBufferMap[tag]--
		} else {
			delete(p.runningBufferMap, tag)
		}
	}
	return nil
}

func (p *Queue) changePart() error {
	p.writerPart += 1
	err := p.openWriter()
	if err != nil {
		p.writerPart--
		log.Errorf("err:%s", err)
		return err
	}
	return nil

}

func (p *Queue) needChangePart(len int) bool {
	if p.writeSize+len > p.config.maxSize {
		return true
	}
	return false
}

/*
func NewQueue(name string, maxsize int) *Queue {
	file, _ := os.OpenFile()
	file.
	return &Queue{
		name:    name,
		maxSize: maxsize,
	}
}

func (queue *Queue) Push(msg Message) {
	if queue.Size()+1 > queue.MaxSize {
		log.Errorf("err:%v", ErrQueueFull)
	} else {
		queue.data = append(queue.data, msg)
		*queue.Qrear += 1
	}
}

func (queue *Queue) Pop() Message {
	msg := queue.data[*queue.Qfront]
	*queue.Qfront -= 1
	return msg
}

func (queue *Queue) Size() int {
	return len(queue.data)
}

func (queue *Queue) Clean() {

}*/
