package ShardReduce

type ShardReduce struct {
	input *map[string]*interface{}
}

func NewShardReduce(input *map[string]*interface{}) *ShardReduce {
	x := ShardReduce{
		input: input,
	}

	return &x
}

func (sr *ShardReduce) NewInput(input *map[string]*interface{}) *ShardReduce {
	sr.input = input
	return sr
}

func (sr *ShardReduce) Filter(fFunc func(string, interface{}) bool) *ShardReduce {
	if len(*sr.input) < 1 {
		return sr
	}
	tempStock := make(map[string]*interface{})
	for k, v := range *sr.input {
		if fFunc(k, *v) == true {
			tempStock[k] = v
		}
	}
	sr.input = &tempStock
	return sr
}

func (sr *ShardReduce) Map(mapFunc func(string, interface{}) interface{}) *ShardReduce {
	if len(*sr.input) < 1 {
		return sr
	}
	tempStock := make(map[string]*interface{})
	for k, v := range *sr.input {
		r := mapFunc(k, *v)
		if r != nil {
			tempStock[k] = &r

		}
	}
	sr.input = &tempStock
	return sr
}

func (sr *ShardReduce) Reduce(reduceFunc func(string, interface{}, string, interface{}) interface{}) interface{} {
	if len(*sr.input) < 1 {
		return sr
	}
	var lastKey string
	var last interface{}

	for k, v := range *sr.input {
		if last != nil {
			last = reduceFunc(lastKey, last, k, *v)
		} else {
			last = *v
			lastKey = k
			continue
		}
	}
	return last
}

func (sr *ShardReduce) Get() *map[string]*interface{} {
	if len(*sr.input) < 1 {
		return sr
	}
	return sr.input
}
