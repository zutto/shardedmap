package ShardFreeze

import (
	"fmt"
	//"github.com/zutto/shardedmap"
	"math/rand"
	"testing"
	"time"
)

func TestFreeze(t *testing.T) {
	/*sh := Shardmap.NewShardMap(128)
	f := NewFreeze("testing", *sh)

	f.Freeze()*/
}

func TestArchiver(t *testing.T) {
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
