package state

import (
	"fmt"
	"github.com/lazygophers/lmq"
	"github.com/lazygophers/log"
	"github.com/lazygophers/utils/app"
	"github.com/lazygophers/utils/routine"
	"github.com/lazygophers/utils/runtime"
	"github.com/lazygophers/utils/unit"
	"google.golang.org/protobuf/proto"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

/*
	.idx	索引文件
	.data    数据文件
	.done	完成文件
*/

type DiskWriter struct {
	name    string
	idxFile *os.File
	idxSize uint64

	dataFile *os.File
	dataSize uint64

	cfg *lmq.DiskQueue
}

func (p *DiskWriter) checkSize() error {
	if p.dataFile != nil {
		stat, err := p.dataFile.Stat()
		if err != nil {
			log.Errorf("err:%v", err)
			return err
		}

		p.dataSize = uint64(stat.Size())
	}

	return nil
}

func (p *DiskWriter) Sync() {
	_ = p.idxFile.Sync()
	_ = p.dataFile.Sync()
}

func (p *DiskWriter) NeedSwitch() bool {
	return p.dataSize >= p.cfg.MaxFilePartSize
}

func (p *DiskWriter) Close() {
	_ = p.idxFile.Close()
	_ = p.dataFile.Close()
}

func (p *DiskWriter) BatchWrite(msgs []*writerMsg) {
	for _, msg := range msgs {
		buffer, err := proto.Marshal(msg.msg)
		if err != nil {
			log.Errorf("err:%v", err)
			msg.err = err
			msg.wg.Done()
			return
		}

		n, err := p.dataFile.Write(buffer)
		if err != nil {
			log.Errorf("err:%v", err)
			msg.err = err
			msg.wg.Done()
			return
		}
		p.dataSize += uint64(n)

		n, err = p.dataFile.WriteString("\n")
		if err != nil {
			log.Errorf("err:%v", err)
			msg.err = err
			msg.wg.Done()
			return
		}
		p.dataSize += uint64(n)

		_, err = p.idxFile.WriteString(msg.msg.MsgId)
		if err != nil {
			log.Errorf("err:%v", err)
			msg.err = err
			msg.wg.Done()
			return
		}

		_, err = p.idxFile.WriteString("\n")
		if err != nil {
			log.Errorf("err:%v", err)
			msg.err = err
			msg.wg.Done()
			return
		}
	}
}

func NewDiskWriter(name string, cfg *lmq.DiskQueue) (*DiskWriter, error) {
	p := &DiskWriter{
		name: name,
		cfg:  cfg,
	}

	var err error
	p.idxFile, err = os.OpenFile(name+".idx", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	p.dataFile, err = os.OpenFile(name+".data", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	return p, nil
}

type DiskQueueWriter struct {
	name string

	cfg *lmq.DiskQueue

	curSeq    string
	writeChan chan *writerMsg

	writer   *DiskWriter
	exitChan chan *sync.WaitGroup
}

func (p *DiskQueueWriter) scanInitFileGroup() error {
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

			if Gt(name, p.curSeq) {
				p.curSeq = name
			}
		}
	}

	return nil
}

type writerMsg struct {
	msg *lmq.Message
	wg  *sync.WaitGroup
	err error
}

func (p *DiskQueueWriter) Write(msg *lmq.Message) error {
	m := &writerMsg{
		msg: msg,
		wg:  &sync.WaitGroup{},
	}
	m.wg.Add(1)
	p.writeChan <- m
	m.wg.Wait()

	if m.err != nil {
		return m.err
	}

	return nil
}

func (p *DiskQueueWriter) openCurrentWriter() (err error) {
	if p.writer != nil {
		return nil
	}

	log.Infof("try opening writer")
	p.writer, err = NewDiskWriter(filepath.Join(runtime.LazyCacheDir(), app.Organization, app.Name, fmt.Sprintf("%s.%s", p.name, p.curSeq)), p.cfg)
	if err != nil {
		log.Errorf("err:%v", err)
		return err
	}

	return nil
}

func (p *DiskQueueWriter) closeCurrentWriter() error {
	p.writer.Sync()
	p.writer.Close()

	p.writer = nil

	return nil
}

func (p *DiskQueueWriter) needSwitchNext() bool {
	if p.writer == nil {
		return false
	}

	if p.writer.NeedSwitch() {
		return true
	}

	return false
}

func (p *DiskQueueWriter) batchWrite(msgs []*writerMsg) {
	failAll := func(err error) {
		for _, msg := range msgs {
			msg.err = err
			msg.wg.Done()
		}
	}
	if p.writer == nil {
		err := p.openCurrentWriter()
		if err != nil {
			failAll(err)
			log.Errorf("err:%v", err)
			return
		}
	}

	if p.needSwitchNext() {
		err := p.closeCurrentWriter()
		if err != nil {
			failAll(err)
			log.Errorf("err:%v", err)
			return
		}

		p.curSeq = Incr(p.curSeq)

		err = p.openCurrentWriter()
		if err != nil {
			failAll(err)
			log.Errorf("err:%v", err)
			return
		}
	}

	p.writer.BatchWrite(msgs)
}

func (p *DiskQueueWriter) ioLoop() error {
	syncTicker := time.NewTicker(time.Second * 5)
	defer syncTicker.Stop()

	checkTicker := time.NewTicker(time.Second * 30)
	defer checkTicker.Stop()

	for {
		select {
		case x := <-p.writeChan:
			var reqList = []*writerMsg{
				x,
			}
			bs := len(x.msg.Data)
			for i := 0; i < 100 && bs < 64*1024*1024; i++ {
				select {
				case x := <-p.writeChan:
					reqList = append(reqList, x)
					bs += len(x.msg.Data)
				default:
					goto mergeExit
				}
			}
		mergeExit:
			p.batchWrite(reqList)

		case <-syncTicker.C:
			if p.writer != nil {
				p.writer.Sync()
			}

		case wg := <-p.exitChan:
			if p.writer != nil {
				p.writer.Sync()
				p.writer.Close()
				p.writer = nil
			}

			wg.Done()

			return nil

		case <-checkTicker.C:
			if p.writer != nil {
				err := p.writer.checkSize()
				if err != nil {
					log.Errorf("err:%v", err)
				}
			}

			if p.needSwitchNext() {
				err := p.closeCurrentWriter()
				if err != nil {
					log.Errorf("err:%v", err)
				}

				p.curSeq = Incr(p.curSeq)

				err = p.openCurrentWriter()
				if err != nil {
					log.Errorf("err:%v", err)
				}
			}
		}
	}
}

func (p *DiskQueueWriter) Close() {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	p.exitChan <- wg
	wg.Wait()
}

func NewDiskQueueWriter(name string, cfg *lmq.DiskQueue) (*DiskQueueWriter, error) {
	p := &DiskQueueWriter{
		name: name,
		cfg:  cfg,

		writeChan: make(chan *writerMsg, 100),
		exitChan:  make(chan *sync.WaitGroup, 1),
	}

	if p.cfg == nil {
		p.cfg = &lmq.DiskQueue{
			MaxFilePartSize: unit.MB * 100,
		}
	}

	err := p.scanInitFileGroup()
	if err != nil {
		log.Errorf("err:%v", err)
		return nil, err
	}

	routine.GoWithMustSuccess(func() (err error) {
		log.SetTrace(fmt.Sprintf("queue-writer.%s.%s", name, log.GetTrace()))

		return p.ioLoop()
	})

	return p, nil
}
