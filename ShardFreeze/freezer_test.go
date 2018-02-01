package ShardFreeze

import (
	"fmt"
	"github.com/zutto/shardedmap"
	"github.com/zutto/shardedmap/ShardReduce"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"
)

var ShardsT int = 16 //more shards = better performane in shardedmap (less I/O mutex lock waiting)
var TestMap Shardmap.ShardMap = Shardmap.NewShardMap(ShardsT)
var gencount int = 10000
var generated bool = false
var runners int = runtime.NumCPU()

var written int = 0
var read int = 0
var dupes int64 = 0

type tS struct {
	A int
	B int
}

func prepare() {

}

func genMap() {
	runners = 2
	if generated {
		return
	}
	if _, err := os.Stat("tmp/"); os.IsNotExist(err) {
		os.MkdirAll("tmp/", os.ModePerm)
	}
	rand.Seed(time.Now().UnixNano())
	wg := sync.WaitGroup{}
	for c := 0; c < runners; c++ {
		wg.Add(1)
		go func() {
			var r string
			for i := 0; i < (gencount / (runners)); i++ {
				var q interface{} = tS{2, i}
				r = RandStringRunes(3)

				TestMap.Set(r, &q)
			}
			wg.Done()
		}()
	}

	wg.Wait()
	for _, shard := range *TestMap.RAW() {
		for range shard.InternalMap {
			written++
		}
	}
	generated = true
	return
}

func TestFreeze(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	genMap()
	freeze := NewFreeze("tmp/testing", &TestMap)
	freeze.FreezeCompleteSet()
	dupes = freeze.GetDupes()

	//freeze.ReindexFreeze()
	//time.Sleep(time.Second * 2)
	println("t done")
}

func TestLoadFreeze(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	genMap()

	xx := Shardmap.NewShardMap(ShardsT)
	freeze := NewFreeze("tmp/testing", &xx)
	//dupes = freeze.GetDupes()
	fmt.Printf("loading..\n")
	freeze.LoadFreeze()
	///dupes = freeze.GetDupes()
	for _, shard := range *xx.RAW() {
		//fmt.Printf("shard\n")
		sr := ShardReduce.NewShardReduce(&shard.InternalMap)
		sr.Filter(func(k string, v interface{}) bool {
			if v != nil {
				read++
			}
			//fmt.Printf("%#v -- %#v\n", k, v)
			return false
		})
	}

	fmt.Printf("loaded from freeze: %d/%d - dupes: %d\n", read, written, dupes)
	time.Sleep(time.Second * 2)
	os.RemoveAll("tmp/")
	os.MkdirAll("tmp/", os.ModePerm)

}

func xx(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	fmt.Printf("test\n")
	a := Archiver{
		name:      "tmp/tartest",
		SizeLimit: 1000 * 1000 * 5,
	}
	a.StartArchiveMapper()
	data1 := []byte("tesing 4555")
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < 1; i++ {
		rndname := RandStringRunes(9)
		a.addFile(rndname, &data1, false)
	}
	//time.Sleep(time.Second * 1)
	//
	//data2 := []byte("tesing 4444")
	//
	/*x, _ := a.GetFile("test")
	fmt.Printf("%#v", string((*x)[:]))
	a.addFile("test", &data1)*/
	a.ReindexFiles()
	//time.Sleep(time.Second * 10)
}

var letterRunes = []rune("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghij")

func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}
