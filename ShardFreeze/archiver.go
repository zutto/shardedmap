package ShardFreeze

import (
	"archive/tar"
	crypto "crypto/rand"
	"fmt"
	//"github.com/zutto/ShardQuest"
	"github.com/zutto/ShardReduce"
	"github.com/zutto/shardedmap"

	//"io/ioutil"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Archiver struct {
	name      string
	SizeLimit int64

	indexMap    Shardmap.ShardMap
	IndexShards int
	indexed     bool
	mapped      bool

	reindexInterval time.Duration

	writeLock sync.Mutex
	mapLock   sync.Mutex
}

type ShardIO struct {
	key   string
	value []byte
}

type fileMap struct {
	diskFileName string
	version      int
	size         int64
	deleted      bool
}

func (a *Archiver) StartArchiveMapper() {
	if a.IndexShards < 1 {
		a.IndexShards = 16
	}

	if a.reindexInterval == 0 {
		a.reindexInterval = time.Second * 2
	}
	a.indexMap = Shardmap.NewShardMap(a.IndexShards)
	go func() {
		for {
			a.mapLock.Lock()
			a.reMap()
			a.mapLock.Unlock()
			a.mapped = true
			time.Sleep(a.reindexInterval)
		}
	}()
}

func (a *Archiver) reMap() {

	files, err := filepath.Glob(a.name + "*.tar")

	if err != nil {
		panic("failed to read files list!") //todo handle properly
	}
	for i := 0; i < len(files); i++ {
		file, err := os.Open(files[i])
		if err != nil {
			panic("failed to open file") //todo handle properly
		}
		tarReader := tar.NewReader(file)
		for {
			h, err := tarReader.Next()
			if err != nil {
				if err.Error() != "EOF" {
					fmt.Printf("err %s - %#v\n", files[i], err.Error())
				}
				break
			}
			ver, vererr := strconv.Atoi(h.Xattrs["version"])
			if vererr != nil {
				ver = 0
			}

			del := false
			if b, err := strconv.ParseBool(h.Xattrs["delete"]); err == nil {
				del = b
			}

			size := 0
			if siz, err := strconv.Atoi(h.Xattrs["size"]); err == nil {
				size = siz
			}

			var fileData interface{} = fileMap{
				diskFileName: files[i],
				version:      ver,
				deleted:      del,
				size:         int64(size),
			}
			if a.indexMap.SetIfNotExist(h.Name, &fileData) == false {
				if (*a.indexMap.Get(h.Name)).(fileMap).version < fileData.(fileMap).version {
					a.indexMap.Set(h.Name, &fileData)
				} else {
				}
			}
		}
	}
}

func (a *Archiver) GetFile(name string) (*tar.Header, *[]byte, error) {
	if a.mapped == false {
		for a.mapped == false {
			time.Sleep(time.Millisecond * 1)
		}
	}
	x := a.indexMap.Get(name)
	if x == nil {
		return nil, nil, errors.New("does not exist")
	}

	file, err := os.OpenFile((*x).(fileMap).diskFileName, os.O_RDONLY, os.ModePerm)
	defer file.Close()
	if err != nil {
		panic("failed to open file")
	}

	tr := tar.NewReader(file)
	var data []byte
	for {
		h, err := tr.Next()
		if err != nil {
			break
		}
		if h.Name == name {
			data = make([]byte, h.Size)
			var read int64 = 0
			for h.Size > read {
				rd, err := tr.Read(data[read:])
				if err != nil {
					panic("failed to read data from tar!")
				}
				read += int64(rd)
			}
			return h, &data, nil
		}
	}

	return nil, nil, errors.New("failed to find the file in tar.. is there re-indexing going on?")

}

func (a *Archiver) addFile(name string, data *[]byte, del bool) {
	a.writeLock.Lock()
	defer a.writeLock.Unlock()

	//wait for mapping to finish before writing data.
	//we need to index the tars to get version data
	if a.mapped == false {
		for a.mapped == false {
			time.Sleep(time.Millisecond * 1)
		}
	}

	ex := a.indexMap.Get(name)
	f := fileMap{
		version: 0,
	}
	if ex != nil {
		f.version = (*ex).(fileMap).version + 1
		f.diskFileName = a.getFreeFile(int64(len(*data)), name, "") //cant have 2 files with same name in same file..
	} else {
		f.diskFileName = a.getFreeFile(int64(len(*data)), "", "")
	}

	headers := tar.Header{
		Name:   name,
		Size:   int64(len(*data)),
		Mode:   0400,
		Xattrs: map[string]string{"version": fmt.Sprintf("%d", f.version), "delete": fmt.Sprintf("%t", del), "size": fmt.Sprintf("%d", len(*data))},
	}

	if _, err := os.Stat(f.diskFileName); os.IsNotExist(err) {
		a.WriteToArchive(f.diskFileName, &headers, data)
	} else {
		a.AppendToArchive(f.diskFileName, &headers, data)
	}
}

func (a *Archiver) getFreeFile(estimate int64, restriction string, baseName string) string {
	files, err := filepath.Glob(a.name + "*tar" + baseName)
	if err != nil {
		panic("failed to read files list!") //todo handle properly
	}
	for i := 0; i < len(files); i++ {
		file, err := os.Stat(files[i])
		if err != nil {
			panic("failed to read file stat data?")
		}

		if file.Size()+estimate < a.SizeLimit {
			//we have to check the file manually, the monitor isnt "real-time" and will be prone to errors.
			if len(restriction) > 0 {
				f, e := os.Open(files[i])
				if e != nil {
					panic("failed to open file")
				}

				tr := tar.NewReader(f)
				found := false
				for {
					h, err := tr.Next()
					if err != nil {
						break
					}
					if h.Name == restriction {
						found = true
						break
					}
				}

				if found == false {
					return files[i]
				}
			} else {
				return files[i]
			}
		}
	}

	//no suitable existing archive found.. add new
	return a.name + "." + a.UUID() + ".tar" + baseName
}

func (a *Archiver) WriteToArchive(tarname string, headers *tar.Header, data *[]byte) {
	x, err := os.OpenFile(tarname, os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		println(err.Error())
		panic("error")
	}

	ta := tar.NewWriter(x)

	defer x.Close()
	defer ta.Close()

	if err := ta.WriteHeader(headers); err != nil {
		fmt.Printf("\n%#v\n", err.Error())
		panic("failed to write headers..")
	}
	if _, err := ta.Write(*data); err != nil {
		panic("failed to write data?")
	}

}

func (a *Archiver) AppendToArchive(tarname string, headers *tar.Header, data *[]byte) {

	if _, err := os.Stat(tarname); os.IsNotExist(err) {
		panic("file doesnt exist!")
	}
	x, err := os.OpenFile(tarname, os.O_RDWR, os.ModePerm)
	if err != nil {
		println(err.Error())
		panic("error")
	}

	if _, err = x.Seek(-2<<9, os.SEEK_END); err != nil {
		println(err.Error())
		panic("failed to get end of file")
	}

	ta := tar.NewWriter(x)
	ta.WriteHeader(headers)
	ta.Write(*data)
	ta.Close()
	x.Close()
}

func (a *Archiver) massAppendToArchive(tarname string, headers []*tar.Header, data []*[]byte) {

	if _, err := os.Stat(tarname); os.IsNotExist(err) {
		panic("file doesnt exist!")
	}
	x, err := os.OpenFile(tarname, os.O_RDWR, os.ModePerm)
	if err != nil {
		println(err.Error())
		panic("error")
	}

	if _, err = x.Seek(-2<<9, os.SEEK_END); err != nil {
		println(err.Error())
		panic("failed to get end of file")
	}

	ta := tar.NewWriter(x)
	for i := 0; i < len(headers); i++ {
		ta.WriteHeader(headers[i])
		ta.Write(*data[i])
	}
	ta.Close()
	x.Close()
}

func (a *Archiver) ReindexFiles() {
	a.writeLock.Lock()
	defer a.writeLock.Unlock()

	a.mapLock.Lock()
	defer a.mapLock.Unlock()
	fmt.Printf("mapping\n")
	a.reMap()
	fmt.Printf("mapping done\n")
	a.mapped = true

	/*
		q := ShardQuest.NewQuest(&a.indexMap)
		q.FindValues(func(i interface{}) bool {
			return i.(fileMap).deleted
		}).Delete() */

	var filesToRename map[string]bool = make(map[string]bool)
	reindexUID := a.UUID() + a.UUID()
	sh := a.indexMap.RAW()

	/*oname := ""
	var hd []*tar.Header = make([]*tar.Header, 0)
	var dd []*[]byte = make([]*[]byte, 0)
	var ovirt int64 = 0*/
	for kx, shard := range *sh {

		x := ShardReduce.NewShardReduce(&(*shard).InternalMap)

		x.Map(func(key string, value interface{}) interface{} {

			var v fileMap = value.(fileMap)
			fn := a.getFreeFile(v.size, "", reindexUID)
			headers, data, geterr := a.GetFile(key)
			if geterr != nil {
				panic("failed to reindex!")
			}

			if _, err := os.Stat(fn); os.IsNotExist(err) {
				a.WriteToArchive(fn, headers, data)
			} else {
				a.AppendToArchive(fn, headers, data)
			}

			filesToRename[fn] = true

			return value
		})
		fmt.Printf("?? done? %#v -- %d\n", kx, a.IndexShards)
	}

	fmt.Printf("-----\n------\ndelete: \n----------\n------\nrename %#v\n\n", filesToRename)
	if len(filesToRename) > 0 {
		files, err := filepath.Glob(a.name + "*.tar")
		if err != nil {
			panic("failed to read files list!") //todo handle properly
		}
		for i := 0; i < len(files); i++ {
			fmt.Printf("removing %s\n", files[i])
			os.Remove(files[i])
		}
		for k, _ := range filesToRename {
			fmt.Printf("rename %s -> %s\n", k, strings.TrimRight(k, reindexUID))
			e := os.Rename(k, strings.Trim(k, reindexUID))
			if e != nil {

				fmt.Println(e.(*os.LinkError).Op)
				fmt.Println(e.(*os.LinkError).Old)
				fmt.Println(e.(*os.LinkError).New)
				fmt.Println(e.(*os.LinkError).Err)
				fmt.Printf("%#v\n", e.Error())
			}

		}
	}
	/*
	   files, err := filepath.Glob(a.name + "*.tar")
	   	if err != nil {
	   		panic("failed to read files list!") //todo handle properly
	   	}
	   	for i := 0; i < len(files); i++ {
	   		file, err := os.Open(files[i])
	   		if err != nil {
	   			panic("failed to open file") //todo handle properly
	   		}

	   		tarReader := tar.NewReader(file)
	   		for {
	   			h, err := tarReader.Next()
	   			if err != nil {
	   				break
	   			}
	   			ver, vererr := strconv.Atoi(h.Xattrs["version"])
	   			if vererr != nil {
	   				ver = 0
	   			}
	   			var fileData interface{} = fileMap{
	   				diskFileName: files[i],
	   				version:      ver,
	   			}
	   			if a.indexMap.SetIfNotExist(h.Name, &fileData) == false {
	   				if (*a.indexMap.Get(h.Name)).(fileMap).version < fileData.(fileMap).version {
	   					a.indexMap.Set(h.Name, &fileData)
	   				}
	   			}
	   		}
	   	}
	*/

}

func (a *Archiver) UUID() string {
	b := make([]byte, 12)
	_, err := crypto.Read(b)
	if err != nil {
		return a.UUID()
	}

	return fmt.Sprintf("%X", b[:]) //b[0:4], b[4:6], b[6:8], b[8:10], b[10:]), nil
}