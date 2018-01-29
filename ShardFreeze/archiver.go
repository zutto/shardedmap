package ShardFreeze

import (
	"archive/tar"
	crypto "crypto/rand"
	"fmt"
	"github.com/zutto/shardedmap/ShardQuest"
	//"github.com/zutto/ShardReduce"
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

	ReindexInterval time.Duration

	writeLock sync.Mutex
	mapLock   sync.Mutex

	dupes int64
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

	if a.ReindexInterval == 0 {
		a.ReindexInterval = time.Second * 2
	}
	a.indexMap = Shardmap.NewShardMap(a.IndexShards)
	go func() {
		for {
			a.mapLock.Lock()
			a.reMap()
			a.mapLock.Unlock()
			a.mapped = true
			time.Sleep(a.ReindexInterval)
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
			fmt.Printf("e %s\n", err.Error())
			panic("failed to open fi qqle") //todo handle properly
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
		file.Close()
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
		panic("failed to open file cc")
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
		a.dupes++
		println("dupe", name)
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
					panic("failed to open file x")
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
	a.reMap()
	a.mapped = true

	//drop entries marked as deleted
	q := ShardQuest.NewQuest(&a.indexMap)
	q.FindValues(func(i interface{}) bool {
		if i.(fileMap).deleted {
			fmt.Printf("dropping %#v", i.(fileMap))
		}
		return i.(fileMap).deleted
	}).Delete()

	var filesToRename map[string]bool = make(map[string]bool)
	var remappedList []string = make([]string, 0)
	reindexUID := a.UUID() + a.UUID()

	files, err := filepath.Glob(a.name + "*.tar")
	if err != nil {
		panic("failed to read files list!") //todo handle properly
	}

	for i := 0; i < len(files); i++ {
		file, err := os.Open(files[i])
		if err != nil {
			panic("failed to open fil ye") //todo handle properly
		}

		rnRequired := true
		remapped := false
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
			x := a.indexMap.Get(h.Name)
			if x != nil {
				if (*x).(fileMap).version == fileData.(fileMap).version {

					//headers
					tda := make([]byte, (*x).(fileMap).size)
					_, err := tarReader.Read(tda)
					if err != nil {
						panic("failed to get a file from tar!")
					}

					/*

					 */
					var target string = ""
					if len(remappedList) < 1 {
						ptf, err := os.Stat(files[i] + reindexUID)
						if err != nil {
							//panic("failed to stat file?")
						} else {
							if ptf.Size()+(*x).(fileMap).size > a.SizeLimit {
								e := os.Rename(files[i]+reindexUID, files[i])
								if e != nil {

									fmt.Println(e.(*os.LinkError).Op)
									fmt.Println(e.(*os.LinkError).Old)
									fmt.Println(e.(*os.LinkError).New)
									fmt.Println(e.(*os.LinkError).Err)
									fmt.Printf("%#v\n", e.Error())
								}
								files[i] = a.name + "." + a.UUID() + ".tar"
							}
						}
						target = files[i] + reindexUID
					} else {
						for i := 0; i < len(remappedList); i++ {
							ptf, err := os.Stat(remappedList[i])
							if err != nil {
								panic("failed to stat file?")
							}
							if ptf.Size()+(*x).(fileMap).size < a.SizeLimit {
								target = remappedList[i]
								//	rnRequired = false
							}
						}

						if target == "" {
							target = files[i] + reindexUID //fallback
						}
					}

					if rnRequired == false {
						fmt.Printf("not writing to new index file, %s\n", target)
						var q fileMap = (*x).(fileMap)
						q.diskFileName = target
						var qi interface{} = q
						a.indexMap.Set(h.Name, &qi)
					}

					headers := tar.Header{
						Name:   h.Name,
						Size:   int64((*x).(fileMap).size),
						Mode:   0400,
						Xattrs: map[string]string{"version": fmt.Sprintf("%d", (*x).(fileMap).version), "delete": fmt.Sprintf("%t", false), "size": fmt.Sprintf("%d", (*x).(fileMap).size)},
					}

					if _, err := os.Stat(target); os.IsNotExist(err) {
						a.WriteToArchive(target, &headers, &tda)
					} else {
						a.AppendToArchive(target, &headers, &tda)
					}
					remapped = true
				} else {
					fmt.Printf("Dropping file %s (size: %d) %#v\n", h.Name, h.Size, h.Xattrs)
				}
			} else {
				fmt.Printf("Dropping file %s (size: %d) %#v\n", h.Name, h.Size, h.Xattrs)
			}
		}

		if remapped {
			os.Remove(files[i])
			if _, err := os.Stat(files[i] + reindexUID); !os.IsNotExist(err) {
				fmt.Printf("remappenings!")
				e := os.Rename(files[i]+reindexUID, files[i])
				if e != nil {

					fmt.Println(e.(*os.LinkError).Op)
					fmt.Println(e.(*os.LinkError).Old)
					fmt.Println(e.(*os.LinkError).New)
					fmt.Println(e.(*os.LinkError).Err)
					fmt.Printf("%#v\n", e.Error())
				}

				remappedList = append(remappedList, files[i])
			}
		}
		file.Close()
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

}

func (a *Archiver) GetList() *[]*Shardmap.Shard {
	if a.mapped == false {
		for a.mapped == false {
			time.Sleep(time.Millisecond * 1)
		}
	}
	println("map back")
	return a.indexMap.RAW()
}

func (a *Archiver) UUID() string {
	b := make([]byte, 12)
	_, err := crypto.Read(b)
	if err != nil {
		return a.UUID()
	}

	return fmt.Sprintf("%X", b[:]) //b[0:4], b[4:6], b[6:8], b[8:10], b[10:]), nil
}

func (a *Archiver) DupeCount() int64 {
	return a.dupes
}
