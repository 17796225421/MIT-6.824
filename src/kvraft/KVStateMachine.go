package kvraft

type KVStateMachine interface {
	Get(key string) (string, Err)
	Put(key, value string) Err
	Append(key, value string) Err
}

type MemoryKV struct {
	KV map[string]string
}

func NewMemoryKV() *MemoryKV {
	return &MemoryKV{make(map[string]string)}
}

func (memoryKV *MemoryKV) Get(key string) (string, Err) {
	// 1. 传入k，传出v
	// 2. 从哈希表找到k对应的v

	if value, ok := memoryKV.KV[key]; ok {
		return value, OK
	}
	return "", ErrNoKey
}

func (memoryKV *MemoryKV) Put(key, value string) Err {
	// 1. 传入kv
	// 2. 哈希表的k对应的值=v

	memoryKV.KV[key] = value
	return OK
}

func (memoryKV *MemoryKV) Append(key, value string) Err {
	// 1. 传入kv
	// 2. 哈希表的k对应的值+=v

	memoryKV.KV[key] += value
	return OK
}
