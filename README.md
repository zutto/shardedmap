# Golangs built in map sharded!
#### For better concurrency.

The whole point of this project was to assist me in a scenario where map access I/O became a bottleneck.

( godoc: https://godoc.org/github.com/zutto/shardedmap )

###1. Installation
`go get github.com/zutto/shardedmap`


###2. Usage
#### Intialization is easy as:
`shardmap := shardedmap.NewShardMap(24)`

 Wondering what that 24 is? It's the amount of 'shards' shardedmap should initialize.
General rule of thumb: Lower your I/O is, the less shards you need. Each shard holds their own mutex lock for rwd access.

##### Adding item to shardmap

```
type demo struct {
   str1 string
   str2 string
}

var data interface{} = demo{"Hello", "World"}

shardmap.Set("I am a string key", &data)
```

Input data type is interface{}, key datatype is always a string.

#####Reading items from shardmap
```
retrievedData := shardmap.Get("I am a string key")
```

###### for more documentation, please look at godoc or the source code!

####Deleting items from shardmap
```
shardmap.Delete("I am a string key")
```

###3. Performance
There are benchmarks comparing normal map vs shardedmap included in the shardmap_test.go file.
Generally, shardedmap only becomes faster the more I/O you require, and amount of shards you are willing to create. Also, don't forget that slices exist! use slices if you can!!

Shards are stored in a slice. The slice where your data is added to is computed by djbhash - djbhash is dubbed as:
>"It is one of the most efficient hash functions ever published."

Djbhash is very simple and insecure hash, its sole purpose is to split the I/O access across the shards, thus why fast hashing function was deemed necessary. 


###4. Limitations
Limitations mainly come from golangs std rwmutex & slices & maps. If you find more limitations in this project, do please let me know!





###----------------


TODO:
- [x] license
- [x] documentation & readme
- [ ] clean the code
- [ ] more tests?
- [ ] better error handling graceful error handling to be done.
- [ ] look for more entrypoints for fuzzing
- [ ] documentation & readme of the sub-libraries (shardreduce & shardquest)







This project is licensed under MIT license.

Also if you use shardedmap somewhere, I would be delighted to hear about it!

Support available via issues, enterprise support available upon request.