package ShardQuest

import (
	"github.com/zutto/shardedmap"
	"math/rand"
	"regexp"
	"runtime"
	"sync"
	"testing"
	"time"
)

type tS struct {
	a int
	b int
}

var ShardsT int = 16 //more shards = better performane in shardedmap (less I/O mutex lock waiting)
var TestMap Shardmap.ShardMap = Shardmap.NewShardMap(ShardsT)
var generated bool = false
var gencount int = 1000
var knownKey string
var q *Quest

func genMap() {
	if generated {
		return
	}
	rand.Seed(time.Now().UnixNano())
	wg := sync.WaitGroup{}
	for c := 0; c < runtime.NumCPU(); c++ {
		wg.Add(1)
		go func() {
			var r string
			for i := 0; i < (gencount / (runtime.NumCPU())); i++ {
				var q interface{} = tS{2, i}
				r = RandStringRunes(5)

				TestMap.Set(r, &q)
			}
			knownKey = r
			wg.Done()
		}()
	}

	wg.Wait()

	q = NewQuest(&TestMap)
	generated = true
}

func BenchmarkFindValues(b *testing.B) {
	generated = false
	genMap()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.FindValues(func(inp interface{}) bool {
			return false
		})
	}
}

func BenchmarkFindKeys(b *testing.B) {
	generated = false
	genMap()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.FindKeys(".*abc.*")
	}
}

func BenchmarkFindKeysContaining(b *testing.B) {
	generated = false
	genMap()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.FindKeysContaining("abc")
	}
}

func BenchmarkFindKeysFunc(b *testing.B) {
	generated = false
	genMap()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.FindKeysFunc(func(k string) bool { return false })
	}
}

func TestFindValues(t *testing.T) {
	genMap()
	q.Reset()
	q.FindValues(func(inp interface{}) bool {
		if inp.(tS).b%5 == 0 {
			return true
		} else {
			return false
		}
	})
}

func TestFindKeys(t *testing.T) {
	genMap()
	q.Reset()

	x := q.FindKeys("^" + regexp.QuoteMeta(knownKey) + "$").Results()
	if len(*x) == 0 {
		t.Error("find keys failed!")
	}
}

func TestFindKeysContaining(t *testing.T) {
	genMap()
	q.Reset()
	x := q.FindKeysContaining(knownKey).Results()
	if len(*x) < 1 {
		t.Error("find keys containing failed!")
	}
}
func TestFindKeysFunc(t *testing.T) {
	genMap()
	q.Reset()
	x := q.FindKeysFunc(func(s string) bool {
		if s == knownKey {
			return true
		} else {
			return false
		}
	}).Results()
	if len(*x) < 1 {
		t.Error("find keys func failed!")
	}
}

func TestDelete(t *testing.T) {
	genMap()
	q.Reset()
	q.FindKeysFunc(func(s string) bool {
		return true
	}).Delete()
	//time.Sleep(time.Millisecond * 1000)
	r := TestMap.RAW()
	for _, shard := range *r {
		if len((*shard).InternalMap) > 0 {
			t.Error("failed to delete keys")
		}
	}
}

var letterRunes = []rune("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789aäöbcdefghij{[}¤$£@/å~^]§klmnopqrstuvwxyz,.%#!*?=")

func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}
