package ShardQuest

import (
	"github.com/zutto/shardedmap"
	"sync"
)

type Quest struct {
	register  *Shardmap.ShardMap
	rawAccess *[]*Shardmap.Shard

	wLock  sync.RWMutex
	result *map[string]*interface{}
	find   Find
}

/*
NewQuest initializes new shardmap quest!
input: shardmap that you want to work on.
Return: a Quest
*/
func NewQuest(i *Shardmap.ShardMap) *Quest {
	if i == nil {
		panic("no map provided!")
	}
	r := make(map[string]*interface{})
	q := Quest{
		register: i,
		result:   &r,
	}

	q.find = Find{}
	q.find.AttachQuest(&q)
	q.gatherCollection()
	return &q
}

func (q *Quest) gatherCollection() {
	q.rawAccess = q.register.RAW()
}

/*
FinValues - a filter function.
Input: a function that accepts interface as argument, the interface is single entry stored in shardmaps shard.
*/
func (q *Quest) FindValues(filterFunc func(interface{}) bool) *Quest {
	q.find.FindValues(filterFunc)
	return q
}

/*
FindKeys - similar what findvalues is, but accepts a regex string for finding keys.

*/
func (q *Quest) FindKeys(rule string) *Quest {
	q.find.FindKeys(rule)
	return q
}

/*
FindKeysContaining - exactly what findkeys is, but uses string.contains, slightly faster.
*/
func (q *Quest) FindKeysContaining(rule string) *Quest {
	q.find.FindKeysContaining(rule)
	return q
}

/*
FindKeysFunc - a custom function to do the string matching.
*/
func (q *Quest) FindKeysFunc(f func(string) bool) *Quest {
	q.find.FindKeysFunc(f)
	return q
}

/*
Results: returns results.
*/
func (q *Quest) Results() *map[string]*interface{} {
	return q.result
}

/*
ResultsToShardedmap - returns a new shardedmap from the results
*/
func (q *Quest) ResultsToShardedmap(shards int) *Shardmap.ShardMap {
	x := Shardmap.NewShardMap(shards)
	for k, v := range *q.result {
		go x.Set(k, v) //shardedmap is async ready
	}
	return &x
}

/*
Delete deletes entries matched with find from the actual asyncmap resourec
*/
func (q *Quest) Delete() {
	q.wLock.RLock()
	defer q.wLock.RUnlock()
	for k, _ := range *q.result {
		go q.register.Delete(k) //shardedmap is async ready
	}
}

func (q *Quest) Reset() {
	x := make(map[string]*interface{})
	q.result = &x
	q.gatherCollection()
}

//you can now run multiple finds() asonchronously on same quest input
func (q *Quest) ToResults(in *map[string]*interface{}) {
	q.wLock.Lock()
	defer q.wLock.Unlock()
	q.mappend(q.result, in)
}

//-

func (f *Quest) mappend(res *map[string]*interface{}, in *map[string]*interface{}) {
	if in == nil {
		return
	}
	for k, v := range *in {
		(*res)[k] = v
	}
	return
}
