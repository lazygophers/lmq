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

type retryType uint8

const (
	fixed retryType = iota
	linear
	exponential
)

var ErrIdxNull = errors.New("idx is null")

type writerReq struct {
	message *Message
	errChan chan error
}

// consumer 写
type doneRq struct {
	msgId     MessageId
	needRetry bool
	needSkip  bool
	baseDelay time.Duration
	errChan   chan error
}

type Config struct {
	path      string
	maxSize   int64
	maxRetry  uint8
	baseDelay time.Duration
	retryType retryType
}

func NewConfig(path string, maxSize int64, maxRetry uint8, baseDelay time.Duration, retryType retryType) *Config {
	return &Config{
		path:      path,
		maxSize:   maxSize,
		maxRetry:  maxRetry,
		baseDelay: baseDelay,
		retryType: retryType,
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
			p.writeToFile(msgs, errChans)
		case dq := <-p.doneChan:
			//写doneReader启动
			dqs := []*doneRq{dq}
			log.Infof("done:%v", dq)
			for {
				select {
				case dq = <-p.doneChan:
					dqs = append(dqs, dq)
					log.Infof("done:%v", dq)
				default:
					goto ENDWD
				}
			}
		ENDWD:
			p.dealDqs(dqs)
		case rq := <-p.readerChan:
			p.readToChan(rq)
		}
	}

}

func (p *Queue) writeToFile(ms []*Message, errChans []chan error) {
	flag := 0

Loop:
	writeSize := p.writeSize
	for i := flag; i < len(ms); i++ {
		ms[i].offset = writeSize
		ms[i].dataLen = int64(len(ms[i].Data))

		//预估大小是否切换切片
		if p.needChangeWritePart(writeSize + ms[i].dataLen) {
			err := p.writeTo(ms[flag:i])
			for _, ch := range errChans {
				ch <- err
			}
			err = p.changeWritePart()
			if err != nil {
				log.Errorf("err:%v", err)
			}
			flag = i
			goto Loop
		}

		writeSize += ms[i].dataLen
	}

	if flag < len(ms) {
		err := p.writeTo(ms[flag:])
		for _, ch := range errChans {
			ch <- err
		}
	}

}

func (p *Queue) readToChan(msgChan chan *Message) {
	//读文件切片启动
	// todo: 判断条件应该为文件是否发生变更
	if p.needChangeReadPart() {
		err := p.changeReadPart()
		if err != nil {
			log.Errorf("err:%s", err)
			return
		}
		log.Debug("file changeReadPart:", p.readerPart)
	}

	err := p.preRead()
	if err != nil {
		log.Errorf("err:%s", err)
	}

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

func (p *Queue) dealDqs(dqs []*doneRq) {
	for _, dq := range dqs {
		if !dq.needRetry {
			p.writeDone(dq)
		} else {
			p.retryMsg(dq)
		}
	}
}

func (p *Queue) writeTo(ms []*Message) error {

	b := log.GetBuffer()
	defer log.PutBuffer(b)

	for _, m := range ms {
		b.Write(m.Data)
	}

	//写data
	_, err := b.WriteTo(p.datWriter)
	if err != nil {
		log.Errorf("err:%s", err)
		return err
	}

	//写index
	size := int64(0)
	for _, m := range ms {
		_, err = m.WriteTo(b)
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}
		size += m.dataLen
	}

	_, err = b.WriteTo(p.idxWriter)
	if err != nil {
		log.Errorf("err:%s", err)
		return err
	}

	p.writeSize = size

	return nil
}

// 预加载idx、对应dat; NewQueue 以及 Loop的readerChan
func (p *Queue) preRead() error {
	fileInfo, err := p.idxReader.Stat()
	if err != nil {
		log.Errorf("err:%s", err)
		return err
	}

	if fileInfo.Size() == 0 {
		return nil
	}

	if fileInfo.Size() >= p.readSize+idxLen {
		err := p.readIdx()
		if err != nil {
			return err
		}
	}

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
		msg := new(Message)
		n, err := msg.ReadFrom(p.idxReader)
		if err != nil {
			log.Errorf("err:%s", err)
			return err
		}

		if !p.readerDone[msg.Id] {
			p.readerBuffer = append(p.readerBuffer, msg)
			flag = true
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

func (p *Queue) writeDone(dq *doneRq) {

	/*b := make([]byte, len(msgIds)*idLen)
	for idx, msgId := range msgIds {
		copy(b[idx*idLen:], msgId[:])
	}

	_, err := p.doneReader.Write(b)
	if err != nil {
		log.Errorf("err:%s", err)
		return err
	}

	for _, msgId := range msgIds {*/
	p.readerDone[dq.msgId] = true
	p.deleteRunningMsg(dq.msgId)
}

func (p *Queue) retryMsg(dq *doneRq) {
	m := p.getRunningMsg(dq.msgId)
	p.deleteRunningMsg(dq.msgId)
	if m.RetryCount == p.config.maxRetry {
		p.writeDone(dq)
	}

	if !dq.needSkip {
		delay := p.config.baseDelay
		if dq.baseDelay != 0 {
			delay = dq.baseDelay
		}
		switch p.config.retryType {
		case fixed:
			m.AccessExecAt = time.Now().Add(delay).Unix()
		case linear:
			m.AccessExecAt = time.Now().Add(delay * time.Duration(m.RetryCount)).Unix()
		case exponential:
			m.AccessExecAt = time.Now().Add(delay * time.Duration(1<<m.RetryCount)).Unix()
		}
	}
	p.readerBuffer = append([]*Message{m}, p.readerBuffer...)
	m.RetryCount++
}

func (p *Queue) getRunningMsg(msgId MessageId) *Message {
	return p.runningMessageMap[msgId]
}

func (p *Queue) deleteRunningMsg(msgId MessageId) {
	m := p.getRunningMsg(msgId)
	delete(p.runningMessageMap, msgId)

	tag := m.Tag
	if p.runningBufferMap[tag] > 1 {
		p.runningBufferMap[tag]--
	} else {
		delete(p.runningBufferMap, tag)
	}

}

func (p *Queue) changeWritePart() error {
	p.writerPart++
	err := p.openWriter()
	if err != nil {
		p.writerPart--
		log.Errorf("err:%s", err)
		return err
	}

	return nil
}

func (p *Queue) needChangeWritePart(len int64) bool {
	if p.writeSize+len > p.config.maxSize {
		return true
	}
	return false
}

func (p *Queue) changeReadPart() error {
	p.readerPart++
	err := p.openReader()
	if err != nil {
		p.readerPart--
		log.Errorf("err:%s", err)
		return err
	}

	err = p.readDone()
	if err != nil {
		return err
	}

	return nil
}

func (p *Queue) needChangeReadPart() bool {
	if p.readerPart == p.writerPart {
		return false
	}

	if len(p.readerBuffer) > 0 {
		return false
	}

	if len(p.runningMessageMap) > 0 {
		return false
	}

	fileInfo, err := p.idxReader.Stat()
	if err != nil {
		log.Errorf("err:%s", err)
		return false
	}

	if p.readSize < fileInfo.Size() {
		return false
	}

	return true
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
