package Shardmap

import (
	"errors"
	"fmt"
	"github.com/google/gofuzz"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"
)

type xy struct {
	Key   string
	Value string
}

type complexXy struct {
	key     string
	value   int
	testing int64
	b       bool
	c       float64
	d       uint32
	y       uint16
}

var a ShardMap = NewShardMap(16) //for bench
var rndLen = 24
var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
var x ShardMap
var Initialized bool = false
var shards = 128
var iterations int = 100000

func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func BenchmarkShardedMapWrite(t *testing.B) {
	wg := sync.WaitGroup{}
	var qq interface{} = xy{
		Key:   "jtn",
		Value: "muuta",
	}
	for i := 0; i < runtime.NumCPU()*2; i++ {
		wg.Add(1)
		go func() {
			for i := 0; i < t.N; i++ {
				a.Set(fmt.Sprintf("%d", i), &qq)
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func BenchmarkShardedMapRead(t *testing.B) {
	wg := sync.WaitGroup{}
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < runtime.NumCPU()*2; i++ {
		wg.Add(1)
		go func() {
			for i := 0; i < t.N; i++ {
				a.Get(fmt.Sprintf("%d", i))
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func BenchmarkShardedMapWR(t *testing.B) {
	wg := sync.WaitGroup{}
	var qq interface{} = xy{
		Key:   "jtn",
		Value: "muuta",
	}
	for i := 0; i < runtime.NumCPU()*2; i++ {
		wg.Add(1)
		go func() {
			for i := 0; i < t.N; i++ {
				str := fmt.Sprintf("%d", i)
				a.Set(str, &qq)
				for a.Get(str) == nil {
					time.Sleep(1 * time.Microsecond)
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func BenchmarkShardedMapWRForceGet(t *testing.B) {
	wg := sync.WaitGroup{}
	var qq interface{} = xy{
		Key:   "jtn",
		Value: "muuta",
	}
	for i := 0; i < runtime.NumCPU()*2; i++ {
		wg.Add(1)
		go func() {
			for i := 0; i < t.N; i++ {
				str := fmt.Sprintf("%d", i)
				a.Set(str, &qq)
				a.ForceGet(str)
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func BenchmarkShardedMapDel(t *testing.B) {
	wg := sync.WaitGroup{}
	for i := 0; i < runtime.NumCPU()*2; i++ {
		wg.Add(1)
		go func() {
			for i := 0; i < t.N; i++ {
				a.Delete(fmt.Sprintf("%d", i))
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

var bnmLock sync.RWMutex
var bnm map[string]*interface{}

func BenchmarkPlainMapWrite(t *testing.B) {
	wg := sync.WaitGroup{}
	bnm = make(map[string]*interface{})
	var qq interface{} = xy{
		Key:   "jtn",
		Value: "muuta",
	}
	for i := 0; i < runtime.NumCPU()*2; i++ {
		wg.Add(1)
		go func() {
			for i := 0; i < t.N; i++ {
				bnmLock.Lock()
				bnm[fmt.Sprintf("%d", i)] = &qq
				bnmLock.Unlock()
			}
			wg.Done()
		}()
	}
	wg.Wait()
}
func BenchmarkPlainMapRead(t *testing.B) {
	wg := sync.WaitGroup{}
	for i := 0; i < runtime.NumCPU()*2; i++ {
		wg.Add(1)
		go func() {
			for i := 0; i < t.N; i++ {
				bnmLock.RLock()
				fmt.Sprintf("", bnm[fmt.Sprintf("%d", i)])
				bnmLock.RUnlock()
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func BenchmarkPlainMapDel(t *testing.B) {
	wg := sync.WaitGroup{}
	for i := 0; i < runtime.NumCPU()*2; i++ {
		wg.Add(1)
		go func() {
			for i := 0; i < t.N; i++ {
				bnmLock.Lock()
				delete(bnm, fmt.Sprintf("%d", i))
				bnmLock.Unlock()
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func TestWithMass(t *testing.T) {

	wga := sync.WaitGroup{}
	if Initialized == false {
		x = NewShardMap(shards)
		Initialized = true
	}
	//fmt.Printf("Spawning %d threads running %d iterations each\n", runtime.NumCPU(), iterations)
	for procs := 0; procs < runtime.NumCPU(); procs++ {
		wga.Add(1)
		go func(proc int) {
			suffix := fmt.Sprintf("t%d-%s\n", proc, RandStringRunes(rndLen))
			for i := 0; i < iterations; i++ {
				err := test(t, suffix)
				if err != nil {
					t.Errorf("FAIL! %s (suffix: %s)", err.Error(), suffix)
					wga.Done()
					return
				}

			}
			wga.Done()
		}(procs)
	}

	wga.Wait()

}

func TestGetSetDel(t *testing.T) {
	if err := test(t, "delta"); err != nil {
		t.Fatal(err.Error())
	}
	if Initialized == false {
		x = NewShardMap(shards)
		Initialized = true
	}
	Initialized = true
}

func TestFuzzInitalize(t *testing.T) {
	for i := 0; i < iterations; i++ {
		f := fuzz.New().NilChance(0)
		var shards int = 0
		f.Fuzz(&shards)
		NewShardMap(shards)
	}
}

func TestFuzzSet(t *testing.T) {
	for rounds := 0; rounds < 5; rounds++ {
		x := NewShardMap(shards)

		for i := 0; i < iterations; i++ {
			f := fuzz.New().NilChance(.25)
			var value complexXy
			var key string
			f.Fuzz(&value)
			f.Fuzz(&key)
			var val interface{} = value
			x.Set(key, &val)
		}
	}
}

func TestFuzzSetGet(t *testing.T) {
	for rounds := 0; rounds < 2; rounds++ {
		x := NewShardMap(shards)

		for i := 0; i < iterations; i++ {
			f := fuzz.New().NilChance(.25)
			var value complexXy
			var key string
			f.Fuzz(&value)
			f.Fuzz(&key)
			var val interface{} = value
			x.Set(key, &val)
			gv := x.Get(key)
			if gv == nil {
				t.Error("nil value?")
			}
			if fmt.Sprintf("%#v", *gv) != fmt.Sprintf("%#v", val) {
				t.Error("values dont match!")
			}
		}
	}
}

func TestFuzzSetGetDel(t *testing.T) {
	for rounds := 0; rounds < 2; rounds++ {
		x := NewShardMap(shards)

		for i := 0; i < iterations; i++ {
			f := fuzz.New().NilChance(.25)
			var value complexXy
			var key string
			f.Fuzz(&value)
			f.Fuzz(&key)
			var val interface{} = value
			x.Set(key, &val)
			gv := x.Get(key)
			if gv == nil {
				t.Error("nil value?")
			}
			if fmt.Sprintf("%#v", *gv) != fmt.Sprintf("%#v", val) {
				t.Error("values dont match!")
			}

			x.Delete(key)
			gv = x.Get(key)
			if gv != nil {
				t.Error("delete failed!")
			}
		}
	}
}

func test(t *testing.T, suffix string) error {
	if Initialized == false {
		for Initialized == false {
			time.Sleep(time.Nanosecond * 1)
		}
	}
	var wr interface{} = xy{
		Key:   "jtn",
		Value: "muuta",
	}

	x.Set("1"+suffix, &wr)
	x.Set("2"+suffix, &wr)
	x.Set("3"+suffix, &wr)

	r1 := x.Get("1" + suffix)
	if err := isNillError(r1, t); err != nil {
		return err
	}
	if fmt.Sprintf("%#v", (*r1)) != fmt.Sprintf("%#v", wr) {
		t.Errorf("Failed to read & write. Wrote: \n(%#v) - Received: \n(%#v)\n", fmt.Sprintf("%#v", (*r1)), fmt.Sprintf("%#v", wr))
	}

	r1 = x.Get("2" + suffix)
	if err := isNillError(r1, t); err != nil {
		return err
	}
	if fmt.Sprintf("%#v", (*r1)) != fmt.Sprintf("%#v", wr) {
		t.Errorf("Failed to read & write. Wrote: \n(%#v) - Received: \n(%#v)\n", fmt.Sprintf("%#v", (*r1)), fmt.Sprintf("%#v", wr))
	}

	r1 = x.Get("3" + suffix)
	if err := isNillError(r1, t); err != nil {
		return err
	}
	if fmt.Sprintf("%#v", (*r1)) != fmt.Sprintf("%#v", wr) {
		t.Errorf("Failed to read & write. Wrote: \n(%#v) - Received: \n(%#v)\n", fmt.Sprintf("%#v", (*r1)), fmt.Sprintf("%#v", wr))
	}

	x.Delete("1" + suffix)
	x.Delete("2" + suffix)
	x.Delete("3" + suffix)
	if x.Get("1"+suffix) != nil {
		t.Error("failed to delete key")
	}

	if x.Get("1"+suffix) != nil {
		t.Error("failed to delete key")
	}

	if x.Get("1"+suffix) != nil {
		t.Error("failed to delete key")
	}
	return nil
}

func isNillError(i *interface{}, t *testing.T) error {
	if i == nil {
		return errors.New("is nil")
	}
	return nil
}
