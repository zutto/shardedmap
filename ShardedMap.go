package Shardmap

import (
	"sync"
	"time"
)

type ShardMap struct {
	shards   int
	shardMap []*shard
}

type shard struct {
	lock        sync.Mutex
	internalMap map[string]*interface{}
}

func NewShardMap(shards int) ShardMap {
	asmap := ShardMap{
		shards:   shards,
		shardMap: make([]*shard, shards),
	}
	//generate empty shards
	for i := 0; i < shards; i++ {
		asmap.shardMap[i] = &shard{
			internalMap: make(map[string]*interface{}),
		}
	}
	return asmap
}

func (a ShardMap) ForceGet(key string) *interface{} {
	for a.Get(key) == nil {
		time.Sleep(time.Microsecond * 1)
	}
	return a.Get(key)
}

func (a ShardMap) LockGet(key string) *interface{} {
	//println("get key: ", key)
	shard := a.DjbHash(key) & uint32(a.shards-1)

	a.shardMap[shard].lock.Lock()
	defer a.shardMap[shard].lock.Unlock()

	return a.shardMap[shard].internalMap[key]
}

func (a ShardMap) Get(key string) *interface{} {
	//println("get key: ", key)
	shard := a.DjbHash(key) & uint32(a.shards-1)
	if a.shardMap[shard] == nil {
		panic("fail!")
	}

	a.shardMap[shard].lock.Lock()
	defer a.shardMap[shard].lock.Unlock()

	return a.shardMap[shard].internalMap[key]
}

func (a ShardMap) Set(key string, data *interface{}) {
	shard := a.DjbHash(key) & uint32(a.shards-1)
	if a.shardMap[shard] == nil {
		panic("fail!")
	}

	a.shardMap[shard].lock.Lock()
	defer a.shardMap[shard].lock.Unlock()

	a.shardMap[shard].internalMap[key] = data
	/*
		if _, found := a.shardMap[shard].internalMap[key]; !found {
			a.Set(key, data)
		} else {
			//fmt.Printf("set success")
		} */
	return
}
func (a ShardMap) Delete(key string) {
	shard := a.DjbHash(key) & uint32(a.shards-1)

	a.shardMap[shard].lock.Lock()
	defer a.shardMap[shard].lock.Unlock()

	delete(a.shardMap[shard].internalMap, key)
	return
}

//
func (a *ShardMap) SetIfNotExist(key string, data *interface{}) bool {
	shard := a.DjbHash(key) & uint32(a.shards-1)

	a.shardMap[shard].lock.Lock()
	defer a.shardMap[shard].lock.Unlock()

	if _, found := a.shardMap[shard].internalMap[key]; found {
		//key found, return back with false
		return false
	} else {
		//key not found, set and return with true
		a.shardMap[shard].internalMap[key] = data
		return true
	}
}

//according to internets, this is fastest hashing algorithm ever made.
//we dont need security, we need distribution which this provides for us.
func (a ShardMap) DjbHash(inp string) uint32 {
	var hash uint32 = 5381 //magic constant, apparently this hash fewest collisions possible.

	for _, chr := range inp {
		hash = ((hash << 5) + hash) + uint32(chr)
	}
	return hash
}
