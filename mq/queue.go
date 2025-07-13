package mq

import (
	"errors"
	"github.com/lazygophers/log"
	"github.com/lazygophers/utils/anyx"
	"github.com/lazygophers/utils/candy"
	"github.com/lazygophers/utils/osx"
	"github.com/lazygophers/utils/runtime"
	"github.com/lazygophers/utils/unit"
	"io"
	"os"
	"path/filepath"
	"regexp"
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

var (
	ErrBufferNull = errors.New("reader buffer is null")
)

type writerReq struct {
	message *Message
	errChan chan error
}

// consumer返回确认消息
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
	retryTime time.Duration
	retryType retryType
}

func (p *Config) apply() {
	if p.path == "" {
		p.path = filepath.Join(runtime.ExecDir(), "data", "mq")
	}

	if p.maxSize <= 0 {
		p.maxSize = unit.MB * 100
	}

	if p.baseDelay <= 0 {
		p.baseDelay = time.Second * 5
	}
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
	pullChan          chan *Message

	exitChan chan chan error

	sorter func(i int, j int) bool
}

func NewQueue(name string, config *Config) (*Queue, error) {
	p := &Queue{
		name:   name,
		config: config,

		writerChan: make(chan *writerReq, 100),
		readerChan: make(chan chan *Message, 100),
		doneChan:   make(chan *doneRq, 100),
		pullChan:   make(chan *Message),
		exitChan:   make(chan chan error, 1),

		readerBuffer:      make([]*Message, 0),
		runningBufferMap:  make(map[uint64]int64),
		runningMessageMap: make(map[MessageId]*Message),
		readerDone:        make(map[MessageId]bool),

		readerPart: 1,
		writerPart: 1,
	}

	config.apply()

	p.sorter = func(i int, j int) bool {
		return true
	}

	//非topic
	matched, _ := regexp.MatchString(`\d$`, p.name)
	if matched {
		p.sorter = func(i, j int) bool {
			return p.readerBuffer[i].Priority > p.readerBuffer[j].Priority
		}
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

	size, _ := p.idxReader.Seek(0, 0)
	p.readSize = size

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

	return <-errChan
}

func (p *Queue) Read() *Message {
	msgChan := make(chan *Message, 1)
	p.readerChan <- msgChan

	return <-msgChan
}

func (p *Queue) Done(dq *doneRq) error {
	p.doneChan <- dq

	return <-dq.errChan
}

func (p *Queue) Pull() chan *Message {
	return p.pullChan
}

func (p *Queue) loop() {
	var rm chan *Message
	var m *Message
	var i int
	var err error
	for {
		if len(p.readerBuffer) > 0 {
			m, i, err = p.readToChan()
			if err != nil {
				log.Errorf("err:%s", err)
				continue
			}
			rm = p.pullChan
		} else {
			rm = nil
			m = nil
		}
		select {
		case wr := <-p.writerChan:
			//写文件切片启动
			msgs := []*Message{wr.message}
			errChans := []chan error{wr.errChan}
			for {
				select {
				case wr = <-p.writerChan:
					msgs = append(msgs, wr.message)
					errChans = append(errChans, wr.errChan)
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
			rq <- m
			if m != nil {
				p.readAfter(m, i)
			}
		case rm <- m:
			p.readAfter(m, i)
		case errChan := <-p.exitChan:
			// 关闭 read chan
			close(p.pullChan)
			// 关闭写文件
			var msgs []*Message
			var errChans []chan error
			for {
				select {
				case wr := <-p.writerChan:
					msgs = append(msgs, wr.message)
					errChans = append(errChans, wr.errChan)
				default:
					goto ENDCLOSE
				}
			}
		ENDCLOSE:
			p.writeToFile(msgs, errChans)
			_ = p.closeWriter()
			// 关闭读文件
			_ = p.closeReader()
			errChan <- nil
		}
	}

}

func (p *Queue) writeToFile(ms []*Message, errChans []chan error) {
	flag := 0

Loop:
	writeSize := p.writeSize
	log.Debugf("Before writesize:%d", writeSize)
	for i := flag; i < len(ms); i++ {
		log.Debugf("msg lenth:%d", len(ms))
		log.Debugf("After i: %d", i)
		// 记录该文件切片的idx索引
		ms[i].Index = int64(i - flag)
		ms[i].offset = writeSize
		ms[i].dataLen = int64(len(ms[i].Data))

		log.Debugf("Write No. %d msg: %v", i, ms[i])
		log.Debug("Writesize:", writeSize)

		//预估大小是否切换切片
		if p.needChangeWritePart(writeSize - p.writeSize + ms[i].dataLen) {
			log.Info("Need change writer part!")
			err := p.writeTo(ms[flag:i])
			for _, ch := range errChans[flag:i] {
				ch <- err
			}
			err = p.changeWritePart()
			if err != nil {
				log.Errorf("err:%v", err)
			}
			log.Debug("flag:", flag)
			flag = i
			goto Loop
		}

		writeSize += ms[i].dataLen
		log.Debugf("After writesize:%d", writeSize)
		log.Debugf("i: %d", i)
	}

	if flag < len(ms) {
		log.Debugf("The last writeTo.")
		err := p.writeTo(ms[flag:])
		for _, ch := range errChans {
			ch <- err
		}
	}

}

func (p *Queue) readToChan() (*Message, int, error) {
	log.Debugf("reader part:%d", p.readerPart)
	if p.needChangeReadPart() {
		err := p.changeReadPart()
		if err != nil {
			log.Errorf("err:%s", err)
			return nil, -1, err
		}
		log.Debug("file changeReadPart:", p.readerPart)
	}

	log.Debugf("reader part:%d", p.readerPart)
	err := p.preRead()
	if err != nil {
		log.Errorf("err:%s", err)
		return nil, -1, err
	}

	log.Debugf("readerBuffer lenth:%d", len(p.readerBuffer))

	for i, v := range p.readerBuffer {
		log.Infof("tag running num:%d", p.runningBufferMap[v.Tag])
		if p.runningBufferMap[v.Tag] >= maxRunningNum {
			log.Infof("> max running num")
			continue
		}

		if v.AccessExecAt > 0 && v.AccessExecAt > time.Now().Unix() {
			continue
		}

		err = p.tryReadData(v)
		log.Debug("readPull msg:", v)
		return v, i, nil
	}

	return nil, -1, nil

}

func (p *Queue) readAfter(m *Message, i int) {
	m.RunningExecAt = time.Now()
	p.runningBufferMap[m.Tag]++
	p.runningMessageMap[m.Id] = m
	p.readerBuffer = candy.RemoveIndex(p.readerBuffer, i)
	log.Infof("readBuffer lenth:%v", len(p.readerBuffer))
}

func (p *Queue) dealDqs(dqs []*doneRq) {
	for _, dq := range dqs {
		log.Debug("dealDq need retry:", dq.needRetry)
		if !dq.needRetry {
			err := p.writeDone(dq.msgId)
			if err != nil {
				log.Errorf("err:%s", err)
				dq.errChan <- err
			} else {
				dq.errChan <- nil
			}
			continue
		}

		m := p.getRunningMsg(dq.msgId)
		p.deleteRunningMsg(dq.msgId)
		log.Debugf("retry msg:%v", m)
		log.Debugf("Before retry count:%d", m.RetryCount)
		if m.RetryCount == p.config.maxRetry {
			log.Debug("RetryCount is max retry count!")
			err := p.writeDone(dq.msgId)
			if err != nil {
				dq.errChan <- err
			} else {
				dq.errChan <- nil
			}
			continue
		}

		if dq.needSkip {
			p.readerBuffer = append([]*Message{m}, p.readerBuffer...)
			continue
		}

		p.retryMsg(m, dq.baseDelay)
		dq.errChan <- nil
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
	log.Debug("current writer size:", p.writeSize)

	return nil
}

// 预加载idx、对应dat; NewQueue 以及 Loop的readerChan
func (p *Queue) preRead() error {
	fileInfo, err := p.idxReader.Stat()
	if err != nil {
		log.Errorf("err:%s", err)
		return err
	}

	/*if fileInfo.Size() == 0 {
		return ErrIdxNull
	}*/

	log.Debugf("fileInfo:%#v", fileInfo)
	log.Debug("current readerSize:", p.readSize)

	if fileInfo.Size() >= p.readSize+idxLen {
		log.Debugf("file size:%d", fileInfo.Size())
		err := p.readIdx()
		if err != nil {
			return err
		}
	}

	if len(p.readerBuffer) == 0 {
		return nil
	}

	log.Debug("readerBuffer[0]:", p.readerBuffer[0])

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
			log.Debugf("read msg:%#v", msg)
			p.readerBuffer = append(p.readerBuffer, msg)
			flag = true
		}

		p.readSize += int64(n)
	}

	if flag {
		sort.Slice(p.readerBuffer, p.sorter)
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

func (p *Queue) writeDone(msgId MessageId) error {
	_, err := p.doneReader.Write(msgId[:])
	if err != nil {
		log.Errorf("err:%s", err)
		return err
	}

	log.Debugf("writeDone:%v", msgId)
	p.readerDone[msgId] = true
	p.deleteRunningMsg(msgId)
	return nil
}

func (p *Queue) retryMsg(m *Message, delay time.Duration) {
	if delay == 0 {
		delay = p.config.baseDelay
	}

	switch p.config.retryType {
	case fixed:
		log.Debug("retry fixed")
		m.AccessExecAt = time.Now().Add(delay).Unix()
	case linear:
		log.Debug("retry linear")
		m.AccessExecAt = time.Now().Add(delay * time.Duration(m.RetryCount)).Unix()
	case exponential:
		log.Debug("retry exponential")
		m.AccessExecAt = time.Now().Add(delay * time.Duration(1<<m.RetryCount)).Unix()
	}

	log.Debugf("Before readerBuffer size:%d", len(p.readerBuffer))
	p.readerBuffer = append([]*Message{m}, p.readerBuffer...)
	log.Debugf("After readerBuffer size:%d", len(p.readerBuffer))
	m.RetryCount++
	log.Debugf("After retry count:%d", m.RetryCount)
}

func (p *Queue) getRunningMsg(msgId MessageId) *Message {
	return p.runningMessageMap[msgId]
}

func (p *Queue) retryRunningMsg() {
	for id, msg := range p.runningMessageMap {
		if msg.RunningExecAt.Add(p.config.retryTime).Before(time.Now()) {
			p.retryMsg(msg, 0)
			p.deleteRunningMsg(id)
		}
	}
}

func (p *Queue) deleteRunningMsg(msgId MessageId) {
	m := p.getRunningMsg(msgId)
	log.Debugf("deleteRunningMsg:%v", m)
	delete(p.runningMessageMap, msgId)

	tag := m.Tag
	log.Debugf("deleteRunningMsg Tag Num:%v", p.runningBufferMap[tag])
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

	log.Debug("change writePart", p.writerPart)
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

	log.Debug("fileInfoSize:", fileInfo.Size())
	log.Debug("readerSize:", p.readSize)

	if p.readSize < fileInfo.Size() {
		log.Debug("false")
		return false
	}

	return true
}

func (p *Queue) transferMsg(dst *Queue, readerBuffer []*Message) error {
	firstMsg := candy.First(readerBuffer)
	lastMsg := candy.Last(readerBuffer)
	if firstMsg == nil || lastMsg == nil {
		// 没有msg 跳过
		return nil
	}

	datBeginOffset := firstMsg.offset
	datEndOffset := lastMsg.offset + lastMsg.dataLen

	idxBeginOffset := firstMsg.Index * idxLen
	idxEndOffset := lastMsg.Index*idxLen + idxLen

	datCursor, _ := p.datReader.Seek(datBeginOffset, 0)
	_, _ = dst.datWriter.Seek(datCursor, 0)

	idxCursor, _ := p.idxWriter.Seek(idxBeginOffset, 0)
	_, _ = dst.idxReader.Seek(idxCursor, 0)

	_, err := io.CopyN(dst.datWriter, p.datReader, datEndOffset-datBeginOffset)
	if err != nil {
		log.Errorf("err:%s", err)
		_, _ = dst.datWriter.Seek(datCursor, 0)
		return err
	}
	// _, _ = dst.datWriter.Seek(datCursor+n, 0)

	_, err = io.CopyN(dst.idxWriter, p.idxReader, idxEndOffset-datBeginOffset)
	if err != nil {
		log.Errorf("err:%s", err)
		_, _ = dst.idxWriter.Seek(idxCursor, 0)
		return err
	}
	// _, _ = dst.idxWriter.Seek(idxCursor+n, 0)

	return nil
}

/*func (p *Queue) updateDone(n int64, begin int64) error {
	_, _ = p.idxReader.Seek(begin, 0)
	for i := int64(0); i < n; i++ {
		msg := new(Message)
		_, err := msg.ReadFrom(p.idxReader)
		if err != nil {
			log.Errorf("err:%s", err)
			return err
		}

		p.readerDone[msg.Id] = true
		err = p.writeDone(msg.Id)
		if err != nil {
			log.Errorf("err:%s", err)
			return err
		}
	}
	return nil
}*/

func (p *Queue) Close() error {
	err := make(chan error, 1)
	p.exitChan <- err
	return <-err
}

func (p *Queue) closeWriter() error {
	if p.datWriter != nil {
		err := p.datWriter.Close()
		if err != nil {
			return err
		}
	}

	if p.idxWriter != nil {
		err := p.idxWriter.Close()
		if err != nil {
			return err
		}
	}

	return nil
}

func (p *Queue) closeReader() error {
	if p.datReader != nil {
		err := p.datReader.Close()
		if err != nil {
			return err
		}
	}
	if p.idxReader != nil {
		err := p.idxReader.Close()
		if err != nil {
			return err
		}
	}
	if p.doneReader != nil {
		err := p.doneReader.Close()
		if err != nil {
			return err
		}
	}

	return nil
}
