package mq

import (
	"errors"
	"github.com/lazygophers/log"
	"github.com/lazygophers/utils/anyx"
	"github.com/lazygophers/utils/candy"
	"github.com/lazygophers/utils/osx"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	readDataNum   = 10
	maxRunningNum = 3
)

var ErrIdxNull = errors.New("idx is null")

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
	maxSize int64
}

func NewConfig(path string, maxSize int64) *Config {
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
	writerPart uint64 //writer文件游标
	writeSize  int64

	datReader  *os.File
	idxReader  *os.File
	doneReader *os.File
	doneChan   chan *doneRq
	readerPart uint64 //reader文件游标
	readSize   int64

	readerBuffer      []*Message
	runningBufferMap  map[uint64]int64
	runningMessageMap map[MessageId]*Message
	readerDone        map[MessageId]bool
	readerChan        chan chan *Message
}

func NewQueue(name string, config *Config) (*Queue, error) {
	p := &Queue{
		name:   name,
		config: config,

		writerChan: make(chan *writerReq, 100),
		readerChan: make(chan chan *Message, 100),

		readerBuffer:      make([]*Message, 0),
		runningBufferMap:  make(map[uint64]int64),
		runningMessageMap: make(map[MessageId]*Message),
		readerDone:        make(map[MessageId]bool),

		readerPart: 1,
		writerPart: 1,
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
		log.Errorf("err:%s", err)
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

	err = p.preRead()
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

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
	p.doneReader, err = os.OpenFile(filepath.Join(p.config.path, anyx.ToString(p.readerPart)+".done"), os.O_RDWR|os.O_CREATE, 0600)
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

	size, _ := p.datWriter.Seek(0, 2)
	p.writeSize = size

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
	// todo: 判断条件应该为文件是否发生变更
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
	writeSize := p.writeSize

	var size int64

	// NOTE: 注意，这里的 b 被 dat 文件以及 idx 文件复用了，但处理 dat 和 idx 是分开的
	b := log.GetBuffer()
	defer log.PutBuffer(b)

	for _, msg := range msgs {
		msg.offset = int64(writeSize)
		msg.dataLen = int64(len(msg.Data))

		size += msg.dataLen

		b.Write(msg.Data)
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
	_, err := b.WriteTo(p.datWriter)
	if err != nil {
		log.Errorf("err:%s", err)
		return err
	}

	//写index
	for _, msg := range msgs {
		log.Infof("msg write to:%v", msg)
		_, err = msg.WriteTo(b)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
	}

	_, err = b.WriteTo(p.idxWriter)
	if err != nil {
		log.Errorf("err:%s", err)
		return err
	}

	p.writeSize = writeSize

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
		log.Errorf("err:%s", ErrIdxNull)
		return ErrIdxNull
	}

	if fileInfo.Size() > p.readSize {
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

	msg.Data = make([]byte, msg.dataLen)
	_, err := p.datReader.ReadAt(msg.Data, msg.offset)
	if err != nil {
		msg.Data = nil
		log.Errorf("err:%s", err)
		return err
	}

	return nil
}

func (p *Queue) readDat() (err error) {
	for i := 0; i < readDataNum && i < len(p.readerBuffer); i++ {
		err = p.tryReadData(p.readerBuffer[i])
		if err != nil {
			log.Errorf("err:%s", err)
			return err
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

	flag := false
	for p.readSize+idxLen <= fileInfo.Size() {
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

		p.readSize += int64(n)
	}

	if flag {
		sort.Slice(p.readerBuffer, func(i, j int) bool {
			return p.readerBuffer[i].Priority > p.readerBuffer[j].Priority
		})
	}

	return nil
}

func (p *Queue) readDone() (err error) {
	for {
		var doneMsgId MessageId
		_, err = p.doneReader.Read(doneMsgId[:])
		if err != nil {
			if err == io.EOF {
				break
			}

			log.Errorf("err:%s", err)
			return err
		}

		p.readerDone[doneMsgId] = true
	}

	return nil
}

func (p *Queue) writeDone(msgIds []MessageId) error {

	b := make([]byte, len(msgIds)*idLen)
	for idx, msgId := range msgIds {
		copy(b[idx*idLen:], msgId[:])
	}

	_, err := p.doneReader.Write(b)
	if err != nil {
		log.Errorf("err:%s", err)
		return err
	}

	for _, msgId := range msgIds {
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
	p.writerPart++
	err := p.openWriter()
	if err != nil {
		p.writerPart--
		log.Errorf("err:%s", err)
		return err
	}

	return nil
}

func (p *Queue) needChangePart(len int64) bool {
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
