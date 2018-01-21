package ShardFreeze

import (
	//"encoding/gob"
	"github.com/zutto/shardedmap"
	//"sync"
)

type Freeze struct {
	shardmap            *Shardmap.ShardMap
	FreezeFileSizeLimit int64
}

type archiveState struct {
	written int64
}

/*
NewFreeze returns a freeze object for freezing shardmap collection into a file(S)
*/
func NewFreeze(freezeName string, s *Shardmap.ShardMap) *Freeze {
	f := Freeze{
		FreezeFileSizeLimit: (1000 * 1000 * 1000 * 1), //1 Gigabyte
	}
	return &f
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
