package ShardReduce

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

type X struct {
	a int
	b int
}

func TestReduce(t *testing.T) {
	var y map[string]*interface{} = FakeMap()

	r := NewShardReduce(&y)

	dones := r.Map(func(key string, input interface{}) interface{} {
		fmt.Printf("%#v\n", input)
		y := input
		return y
	}).Filter(func(key string, input interface{}) bool {
		rand.Seed(time.Now().UnixNano())
		rnd := rand.Intn(100)
		if rnd > 50 {
			return true
		} else {
			return false
		}
	}).Filter(func(key string, input interface{}) bool {
		rand.Seed(time.Now().UnixNano())
		rnd := rand.Intn(100)
		if rnd > 50 {
			return true
		} else {
			return false
		}
	}).Map(func(key string, input interface{}) interface{} {
		y := input
		fmt.Printf("%#v\n", input)
		return y
	}).Reduce(func(startkey string, start interface{}, key string, ne interface{}) interface{} {
		var brandNew X = X{start.(X).a + ne.(X).a, start.(X).b + ne.(X).b}
		return brandNew
	})

	fmt.Printf("---\nresult: %#v", dones)

}

func FakeMap() map[string]*interface{} {
	x := make(map[string]*interface{})
	for y := 0; y < 25; y++ {
		var q interface{}
		q = X{y, y + 1}
		x[fmt.Sprint("%d", y*5534)] = &q
	}

	return x
}
