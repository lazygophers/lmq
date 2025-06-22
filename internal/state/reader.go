package state

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"github.com/RoaringBitmap/roaring/v2"
	"github.com/lazygophers/lmq"
	"github.com/lazygophers/log"
	"github.com/lazygophers/utils/app"
	"github.com/lazygophers/utils/routine"
	"github.com/lazygophers/utils/runtime"
	"go.uber.org/atomic"
	"google.golang.org/protobuf/proto"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type DiskReader struct {
	name string
	seq  string

	recountIndex bool

	idxFile  *os.File
	dataFile *os.File
	doneFile *os.File

	index    *roaring.Bitmap
	indexCnt *atomic.Uint64

	done    *roaring.Bitmap
	doneCnt *atomic.Uint64
}

func (p *DiskReader) Done(msgId string) {
	_, err := p.doneFile.WriteString(msgId)
	if err != nil {
		log.Errorf("err:%v", err)
		return
	}

	_, err = p.doneFile.WriteString("\n")
	if err != nil {
		log.Errorf("err:%v", err)
		return
	}

	p.done.Add(binary.LittleEndian.Uint32([]byte(msgId)))
	p.doneCnt.Inc()
}

func (p *DiskReader) isFinished() bool {
	return p.indexCnt.Load() == p.doneCnt.Load()
}

func (p *DiskReader) Close() {
	_ = p.idxFile.Close()
	_ = p.dataFile.Close()
	_ = p.doneFile.Close()
}

func (p *DiskReader) loadIndex() error {
	if p.idxFile == nil {
		return nil
	}

	reader := bufio.NewScanner(p.idxFile)
	for reader.Scan() {
		line := reader.Bytes()
		if len(line) == 0 {
			continue
		}

		c := binary.LittleEndian.Uint32(line)
		if !p.index.Contains(c) {
			p.index.Add(c)
			p.indexCnt.Inc()
		}
	}

	return nil
}

func (p *DiskReader) loadDoned() error {
	if p.doneFile == nil {
		return nil
	}

	reader := bufio.NewScanner(p.doneFile)
	for reader.Scan() {
		line := reader.Bytes()
		if len(line) == 0 {
			continue
		}

		c := binary.LittleEndian.Uint32(line)
		if !p.done.Contains(c) {
			p.done.Add(c)
			p.doneCnt.Inc()
		}
	}

	return nil
}

func (p *DiskReader) Pop() *lmq.Message {
	var msg lmq.Message
	var err error

	reader := bufio.NewScanner(p.dataFile)
	for reader.Scan() {
		line := reader.Bytes()
		if len(line) == 0 {
			continue
		}

		err = proto.Unmarshal(line, &msg)
		if err != nil {
			log.Errorf("err:%v", err)
			continue
		}

		if p.done.Contains(binary.LittleEndian.Uint32([]byte(msg.MsgId))) {
			continue
		}

		return &msg
	}

	return nil
}

func NewDiskReader(name, seq string) (*DiskReader, error) {
	p := &DiskReader{
		name:     name,
		seq:      seq,
		idxFile:  nil,
		dataFile: nil,
		doneFile: nil,
		index:    roaring.New(),
		indexCnt: atomic.NewUint64(0),
		done:     roaring.New(),
		doneCnt:  atomic.NewUint64(0),
	}

	var err error
	p.idxFile, err = os.Open(filepath.Join(runtime.LazyCacheDir(), app.Organization, app.Name, fmt.Sprintf("%s.%s.idx", name, seq)))
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	_, err = p.idxFile.Seek(0, io.SeekStart)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	err = p.loadIndex()
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	p.doneFile, err = os.OpenFile(filepath.Join(runtime.LazyCacheDir(), app.Organization, app.Name, fmt.Sprintf("%s.%s.done", name, seq)), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	_, err = p.doneFile.Seek(0, io.SeekStart)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	err = p.loadDoned()
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	p.dataFile, err = os.Open(filepath.Join(runtime.LazyCacheDir(), app.Organization, app.Name, fmt.Sprintf("%s.%s.data", name, seq)))
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	_, err = p.doneFile.Seek(0, io.SeekStart)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	return p, nil
}

type readerMsg struct {
	msg *lmq.Message
	seq string
	wg  *sync.WaitGroup
}

type DiskQueueReader struct {
	name string

	readers []*DiskReader

	minSeq string
	maxSeq string

	notifyWrite chan string
	msgChan     chan *readerMsg
	doneChan    chan *readerMsg
}

func (p *DiskQueueReader) scanInitFileGroup() error {
	stat, err := os.Stat(filepath.Join(runtime.LazyCacheDir(), app.Organization, app.Name))
	if err != nil {
		if os.IsNotExist(err) {
			err = os.MkdirAll(filepath.Join(runtime.LazyCacheDir(), app.Organization, app.Name), 0777)
			if err != nil {
				log.Errorf("err:%v", err)
				return err
			}
		} else {
			log.Errorf("err:%v", err)
			return err
		}
	} else {
		if !stat.IsDir() {
			return fmt.Errorf("%s is not dir", filepath.Join(runtime.LazyCacheDir(), app.Organization, app.Name))
		}

		fileList, err := os.ReadDir(filepath.Join(runtime.LazyCacheDir(), app.Organization, app.Name))
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}

		namePrefix := p.name + "."

		for _, file := range fileList {
			if file.IsDir() {
				continue
			}

			name := file.Name()

			if !strings.HasPrefix(name, namePrefix) {
				continue
			}
			name = strings.TrimPrefix(name, namePrefix)

			if strings.HasSuffix(name, ".idx") {
				continue
			}
			name = strings.TrimPrefix(name, ".idx")

			if !IsNumber(name) {
				continue
			}

			if Gt(name, p.maxSeq) {
				p.maxSeq = name
			}

			if Lt(name, p.minSeq) {
				p.minSeq = name
			}
		}
	}

	return nil
}

func (p *DiskQueueReader) cleanFinishedReader() {
	if len(p.readers) <= 1 {
		return
	}

	var n []*DiskReader
	for idx, reader := range p.readers {
		if idx+1 == len(p.readers) {
			n = append(n, reader)
			continue
		}
		if !reader.isFinished() {
			n = append(n, reader)
			continue
		}

	}

	if len(n) == len(p.readers) {
		return
	}

	p.readers = n
}

func (p *DiskQueueReader) isReaderOpened(seq string) bool {
	for _, reader := range p.readers {
		if reader.seq == seq {
			return true
		}
	}
	return false
}

func (p *DiskQueueReader) openReader() error {
	p.cleanFinishedReader()

	if p.minSeq == "" {
		return nil
	}

	for len(p.readers) < 1 && Lte(p.minSeq, p.maxSeq) {
		if !p.isReaderOpened(p.minSeq) {
			reader, err := NewDiskReader(p.name, p.minSeq)
			if err != nil {
				if !os.IsNotExist(err) {
					log.Errorf("err:%v", err)
					return err
				}
			} else {
				if p.minSeq != p.maxSeq && reader.isFinished() {
					log.Infof("%s %s is finished,skip", p.name, p.minSeq)
					reader.Close()
				}
			}
		}

		if p.minSeq < p.maxSeq {
			p.minSeq = Incr(p.minSeq)
		} else {
			break
		}
	}

	return nil
}

func (p *DiskQueueReader) Pop() *readerMsg {
	msg := &readerMsg{
		wg: &sync.WaitGroup{},
	}
	msg.wg.Add(1)
	p.msgChan <- msg
	msg.wg.Wait()
	return msg
}

func (p *DiskQueueReader) ioLoop() error {
	log.Infof("start io loop")

	for {
		select {
		case seq := <-p.notifyWrite:
			for _, reader := range p.readers {
				if reader.seq == seq {
					reader.recountIndex = true
					break
				}
			}

			if Gt(seq, p.maxSeq) {
				p.maxSeq = seq
			}

		case x := <-p.msgChan:
			for _, reader := range p.readers {
				x.msg = reader.Pop()
				if x != nil {
					x.seq = reader.seq
					break
				}
			}

			x.wg.Done()

		case x := <-p.doneChan:
			for _, reader := range p.readers {
				if reader.seq == x.seq {
					reader.Done(x.msg.MsgId)
					break
				}
			}

			x.wg.Done()
		}
	}
}

func NewDiskQueueReader(name string) (*DiskQueueReader, error) {
	p := &DiskQueueReader{
		name: name,

		notifyWrite: make(chan string, 10),
		msgChan:     make(chan *readerMsg, 10),
		doneChan:    make(chan *readerMsg, 10),
	}

	err := p.scanInitFileGroup()
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	err = p.openReader()
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	routine.GoWithMustSuccess(func() (err error) {
		log.SetTrace(fmt.Sprintf("queue-reader.%s.%s", name, log.GetTrace()))

		return p.ioLoop()
	})

	return p, nil
}
