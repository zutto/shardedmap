package Shardmap

import (
	"sync"
	"time"
)

type ShardMap struct {
	shards   int
	shardMap []*Shard
	Keys     int
}

type Shard struct {
	Lock        sync.RWMutex
	InternalMap map[string]*interface{}
}

//NewShardMap initializes new Shardmap
func NewShardMap(shards int) ShardMap {
	asmap := ShardMap{
		shards:   shards,
		shardMap: make([]*Shard, shards),
		Keys:     0,
	}
	//generate empty shards
	for i := 0; i < shards; i++ {
		asmap.shardMap[i] = &Shard{
			InternalMap: make(map[string]*interface{}),
		}
	}
	return asmap
}

func (a *ShardMap) RAW() *[]*Shard {
	return &a.shardMap
}

func (a *ShardMap) DumpKeys() {
	println("-----------------")
	println("dumping keys")
	println("-----------------")
	for shardid, sh := range a.shardMap {
		sh.Lock.Lock()
		for key, _ := range (*sh).InternalMap {
			println("shard key:", key, "on shard:", shardid)
		}
		sh.Lock.Unlock()
	}
	println("-----------------")
	println("dumping keys end")
	println("-----------------")
}

//ForceGet is for concurrent write-read.
//if you expect a entry to be written on another goroutine - this function is handy for waiting that entry to appear
//Do note - theres the loop of this as of now, and this will at minimum do 2 Get calls.
func (a *ShardMap) ForceGet(key string) *interface{} {
	for a.Get(key) == nil {
		time.Sleep(time.Microsecond * 1)
	}
	return a.Get(key)
}

//LockGet is yet another concurrent helper function.
//Lockget is for using sync.rwmutex.lock instead of sync.rwmutex.rlock for reading.
func (a *ShardMap) LockGet(key string) *interface{} {
	//println("get key: ", key)
	shard := a.DjbHash(key) & uint32(a.shards-1)

	a.shardMap[shard].Lock.Lock()
	defer a.shardMap[shard].Lock.Unlock()

	return a.shardMap[shard].InternalMap[key]
}

//Get returns entry from the sharded map
func (a *ShardMap) Get(key string) *interface{} {
	//println("get key: ", key)
	shard := a.DjbHash(key) & uint32(a.shards-1)
	if a.shardMap[shard] == nil {
		panic("fail!")
	}

	a.shardMap[shard].Lock.RLock()
	defer a.shardMap[shard].Lock.RUnlock()

	return a.shardMap[shard].InternalMap[key]
}

//Set sets an entry into the sharded map
func (a *ShardMap) Set(key string, data *interface{}) {
	shard := a.DjbHash(key) & uint32(a.shards-1)
	if a.shardMap[shard] == nil {
		panic("fail!")
	}

	a.shardMap[shard].Lock.Lock()
	defer a.shardMap[shard].Lock.Unlock()

	a.shardMap[shard].InternalMap[key] = data
	/*
		if _, found := a.shardMap[shard].InternalMap[key]; !found {
			a.Set(key, data)
		} else {
			//fmt.Printf("set success")
		} */
	return
}

//Delete deletes an antry from the sharded map - if the entry doesnt exist, it does nothing.
func (a *ShardMap) Delete(key string) {
	shard := a.DjbHash(key) & uint32(a.shards-1)

	a.shardMap[shard].Lock.Lock()
	defer a.shardMap[shard].Lock.Unlock()

	delete(a.shardMap[shard].InternalMap, key)
	return
}

//SetIfNotExist is also another concurrency helper function.
//SetIfNotExist will set value if it does not exist yet, otherwise it will do nothing
//the function will return true on success, and false if the key already exists.
func (a *ShardMap) SetIfNotExist(key string, data *interface{}) bool {
	shard := a.DjbHash(key) & uint32(a.shards-1)

	a.shardMap[shard].Lock.Lock()
	defer a.shardMap[shard].Lock.Unlock()

	if _, found := a.shardMap[shard].InternalMap[key]; found {
		//key found, return back with false
		return false
	} else {
		//key not found, set and return with true
		a.shardMap[shard].InternalMap[key] = data
		return true
	}
}

//DjbHash is for sharding the map.
//according to internets, this is fastest hashing algorithm ever made.
//we dont need security, we need distribution which this provides for us.
func (a *ShardMap) DjbHash(inp string) uint32 {
	var hash uint32 = 5381 //magic constant, apparently this hash fewest collisions possible.

	for _, chr := range inp {
		hash = ((hash << 5) + hash) + uint32(chr)
	}
	return hash
}
