package ShardFreeze

import (
	//"bytes"
	"encoding/json"
	//"fmt"
	"github.com/zutto/shardedmap"
	"github.com/zutto/shardedmap/ShardReduce"
	//"io/ioutil"
	"sync"
	"time"
)

type Freeze struct {
	name                string
	shardmap            *Shardmap.ShardMap
	FreezeFileSizeLimit int64
	archiver            Archiver
}

type archiveState struct {
	written int64
}

/*
NewFreeze returns a freeze object for freezing shardmap collection into a file(S)
*/
func NewFreeze(freezeName string, s *Shardmap.ShardMap) *Freeze {
	f := Freeze{
		name:                freezeName,
		FreezeFileSizeLimit: (1000 * 10), //1 Gigabyte
		shardmap:            s,
	}
	a := Archiver{
		name:            freezeName,
		SizeLimit:       f.FreezeFileSizeLimit,
		ReindexInterval: time.Second * 1,
		IndexShards:     16,
	}

	f.archiver = a
	go f.archiver.StartArchiveMapper()
	return &f
}

func (f *Freeze) ReindexFreeze() {
	f.archiver.ReindexFiles()
}

func (f *Freeze) GetDupes() int64 {
	return f.archiver.DupeCount()
}

func (f *Freeze) FreezeCompleteSet() {
	for _, shard := range *f.shardmap.RAW() {
		sr := ShardReduce.NewShardReduce(&(shard.InternalMap))
		sr.Map(func(key string, value interface{}) interface{} {

			f.Set(key, value)

			return value
		})
	}
}

func (f *Freeze) gobInterface(i interface{}) *[]byte {
	b, err := json.Marshal(i.(interface{}))
	if err != nil {
		return nil
	}
	return &b
}

func (f *Freeze) interfaceGob(data *[]byte) interface{} {
	var x interface{}
	err := json.Unmarshal(*data, &x)
	if err != nil {

		return x
	}

	return x
}

func (f *Freeze) LoadFreeze() {
	s := sync.WaitGroup{}
	l := *f.archiver.GetList()
	for _, shard := range l {
		println("loading freeze")
		s.Add(1)
		go func(shard *Shardmap.Shard) {
			sr := ShardReduce.NewShardReduce(&(shard.InternalMap))
			sr.Map(func(key string, value interface{}) interface{} {
				data := f.Get(key)
				(*f.shardmap).Set(key, &(data))
				return value
			})
			println("load done")
			s.Done()
		}(shard)
	}
	s.Wait()
}

func (f *Freeze) Set(key string, data interface{}) {
	f.archiver.addFile(key, f.gobInterface(data), false)
}

func (f *Freeze) Get(key string) interface{} {
	_, file, _ := f.archiver.GetFile(key)
	return f.interfaceGob(file)

}

func (f *Freeze) Delete(key string) {
	d := make([]byte, 0)
	f.archiver.addFile(key, &d, true)
}

func (f *Freeze) Freeze() {
}

/*
	start, doneWork, done := f.fvQueue()
	wg := sync.WaitGroup{}
	for _, shard := range *f.shardmap.RAW() {
		wg.Add(1)
		<-start
		go func() {
			for k, v := range *shard.InternalMap {

			}
			wg.Done()
		}()
	}

	wg.Wait()
	done <- true
}

func (f *Freeze) archiveWriterProxy() {

}

func (f *Freeze) fvQueue() (startWork chan bool, doneWork chan bool, done chan bool) {
	startWork = make(chan bool, f.MaxWorkers)
	doneWork = make(chan bool, f.MaxWorkers)
	done = make(chan bool, 0)

	go func() {
		var loaded int = 0
		for {
			select {
			case <-done:
				return
			case <-doneWork:
				loaded--
			default:
				if loaded < f.MaxWorkers {
					loaded++
					startWork <- true
				}
			}
		}
	}()
	return
}

*/
