package ShardQuest

import (
	"github.com/zutto/ShardReduce"
	"github.com/zutto/shardedmap"
	"regexp"
	"runtime"
	"strings"
	"sync"
)

type Find struct {
	q          *Quest
	MaxWorkers int
}

func (f *Find) fvQueue() (startWork chan bool, doneWork chan bool, done chan bool) {
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

func (f *Find) FindValues(filterFunc func(interface{}) bool) {
	StartWork, DoneWork, done := f.fvQueue()
	wg := sync.WaitGroup{}
	for _, shard := range *f.q.rawAccess {
		<-StartWork

		wg.Add(1)
		go func(shard *Shardmap.Shard) {
			(*shard).Lock.RLock()
			defer (*shard).Lock.RUnlock()
			defer wg.Done()
			sr := ShardReduce.NewShardReduce((&(*shard).InternalMap))
			//handle

			sr.Filter(func(k string, i interface{}) bool {
				return filterFunc(i)
			})

			f.q.ToResults(sr.Get())
			DoneWork <- true
		}(shard)
	}

	wg.Wait()
	done <- true
}

func (f *Find) FindKeys(rule string) {
	StartWork, DoneWork, done := f.fvQueue()
	wg := sync.WaitGroup{}
	reg, err := regexp.Compile(rule)
	if err != nil {
		return
	}

	for _, shard := range *f.q.rawAccess {
		<-StartWork
		wg.Add(1)
		go func(shard *Shardmap.Shard) {
			if shard != nil {
				(*shard).Lock.RLock()

				//handle
				sr := ShardReduce.NewShardReduce(&(*shard).InternalMap)
				sr.Filter(func(k string, i interface{}) bool {
					if reg.FindString(k) != "" {
						return true
					}

					return false
				})
				defer (*shard).Lock.RUnlock()
				//result

				f.q.ToResults(sr.Get())
			}
			DoneWork <- true
			wg.Done()
		}(shard)

	}

	wg.Wait()
	done <- true
}

func (f *Find) FindKeysFunc(fun func(string) bool) {
	StartWork, DoneWork, done := f.fvQueue()
	wg := sync.WaitGroup{}

	for _, shard := range *f.q.rawAccess {
		<-StartWork
		wg.Add(1)
		go func(shard *Shardmap.Shard) {
			if shard != nil {
				(*shard).Lock.RLock()

				//handle
				sr := ShardReduce.NewShardReduce(&(*shard).InternalMap)
				sr.Filter(func(k string, i interface{}) bool {
					return fun(k)
				})
				defer (*shard).Lock.RUnlock()
				//result

				f.q.ToResults(sr.Get())
			}
			DoneWork <- true
			wg.Done()
		}(shard)

	}

	wg.Wait()
	done <- true
}

func (f *Find) FindKeysContaining(rule string) {
	StartWork, DoneWork, done := f.fvQueue()
	wg := sync.WaitGroup{}

	for _, shard := range *f.q.rawAccess {
		<-StartWork
		wg.Add(1)
		go func(shard *Shardmap.Shard) {
			if shard != nil {
				(*shard).Lock.RLock()

				//handle
				sr := ShardReduce.NewShardReduce(&(*shard).InternalMap)
				sr.Filter(func(k string, i interface{}) bool {
					return strings.Contains(k, rule)
				})
				defer (*shard).Lock.RUnlock()
				//result

				f.q.ToResults(sr.Get())
			}
			DoneWork <- true
			wg.Done()
		}(shard)

	}

	wg.Wait()
	done <- true
}

func (f *Find) AttachQuest(q *Quest) {
	f.MaxWorkers = runtime.NumCPU() * 10 //??
	f.q = q
}
