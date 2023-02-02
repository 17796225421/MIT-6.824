package shardctrler

import (
	"sort"
)

type ConfigStateMachine interface {
	Join(groups map[int][]string) Err
	Leave(gids []int) Err
	Move(shard, gid int) Err
	Query(num int) (Config, Err)
}

type MemoryConfigStateMachine struct {
	Configs []Config
}

func NewMemoryConfigStateMachine() *MemoryConfigStateMachine {
	cf := &MemoryConfigStateMachine{make([]Config, 1)}
	cf.Configs[0] = DefaultConfig()
	return cf
}

func (cf *MemoryConfigStateMachine) Join(groups map[int][]string) Err {
	// 1. 传入组哈希表
	// 2. 从配置数组获得最后配置，构造新配置
	// 3. 遍历组哈希表，对于当前的组编号和组数组
	//   a. 对应新配置的组哈希表，如果新配置的组哈希表没有当前组编号，对当前组数组拷贝，将拷贝组数组作为新配置的组哈希表的当前组编号对应的组数组。
	//   b. 通过新配置获得哈希表shard对应的组数组
	// 4. 循环，从哈希表shard对应的组数组取出shard最多的组数组和shard最少的组数组
	//   a. 如果两个组数组的shard相差小于等于1，退出循环
	//   b. 将shard最多的组数组的一个shard交给shard最少的组数组

	lastConfig := cf.Configs[len(cf.Configs)-1]
	newConfig := Config{len(cf.Configs), lastConfig.Shards, deepCopy(lastConfig.Groups)}
	for gid, servers := range groups {
		if _, ok := newConfig.Groups[gid]; !ok {
			newServers := make([]string, len(servers))
			copy(newServers, servers)
			newConfig.Groups[gid] = newServers
		}
	}
	s2g := Group2Shards(newConfig)
	for {
		source, target := GetGIDWithMaximumShards(s2g), GetGIDWithMinimumShards(s2g)
		if source != 0 && len(s2g[source])-len(s2g[target]) <= 1 {
			break
		}
		s2g[target] = append(s2g[target], s2g[source][0])
		s2g[source] = s2g[source][1:]
	}
	var newShards [NShards]int
	for gid, shards := range s2g {
		for _, shard := range shards {
			newShards[shard] = gid
		}
	}
	newConfig.Shards = newShards
	cf.Configs = append(cf.Configs, newConfig)
	return OK
}

func (cf *MemoryConfigStateMachine) Leave(gids []int) Err {
	// 1. 传入组数组
	// 2. 从配置数组获得最后配置，构造新配置
	// 3. 通过新配置获得哈希表shard对应的组数组
	// 4. 遍历组数组，对于遍历组，如果在新配置的组哈希表，从新配置的组哈希表删除？

	lastConfig := cf.Configs[len(cf.Configs)-1]
	newConfig := Config{len(cf.Configs), lastConfig.Shards, deepCopy(lastConfig.Groups)}
	s2g := Group2Shards(newConfig)
	orphanShards := make([]int, 0)
	for _, gid := range gids {
		if _, ok := newConfig.Groups[gid]; ok {
			delete(newConfig.Groups, gid)
		}
		if shards, ok := s2g[gid]; ok {
			orphanShards = append(orphanShards, shards...)
			delete(s2g, gid)
		}
	}
	var newShards [NShards]int
	// load balancing is performed only when raft groups exist
	if len(newConfig.Groups) != 0 {
		for _, shard := range orphanShards {
			target := GetGIDWithMinimumShards(s2g)
			s2g[target] = append(s2g[target], shard)
		}
		for gid, shards := range s2g {
			for _, shard := range shards {
				newShards[shard] = gid
			}
		}
	}
	newConfig.Shards = newShards
	cf.Configs = append(cf.Configs, newConfig)
	return OK
}

func (cf *MemoryConfigStateMachine) Move(shard, gid int) Err {
	// 1. 传入shard、组
	// 2. 从配置数组获得最后配置，构造新配置
	// 3. 新配置的哈希表shard对应的组设置为传入组

	lastConfig := cf.Configs[len(cf.Configs)-1]
	newConfig := Config{len(cf.Configs), lastConfig.Shards, deepCopy(lastConfig.Groups)}
	newConfig.Shards[shard] = gid
	cf.Configs = append(cf.Configs, newConfig)
	return OK
}

func (cf *MemoryConfigStateMachine) Query(num int) (Config, Err) {
	// 1. 传入num，传出配置数组下标num的配置

	if num < 0 || num >= len(cf.Configs) {
		return cf.Configs[len(cf.Configs)-1], OK
	}
	return cf.Configs[num], OK
}

func Group2Shards(config Config) map[int][]int {
	s2g := make(map[int][]int)
	for gid := range config.Groups {
		s2g[gid] = make([]int, 0)
	}
	for shard, gid := range config.Shards {
		s2g[gid] = append(s2g[gid], shard)
	}
	return s2g
}

func GetGIDWithMinimumShards(s2g map[int][]int) int {
	// make iteration deterministic
	var keys []int
	for k := range s2g {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	// find GID with minimum shards
	index, min := -1, NShards+1
	for _, gid := range keys {
		if gid != 0 && len(s2g[gid]) < min {
			index, min = gid, len(s2g[gid])
		}
	}
	return index
}

func GetGIDWithMaximumShards(s2g map[int][]int) int {
	// always choose gid 0 if there is any
	if shards, ok := s2g[0]; ok && len(shards) > 0 {
		return 0
	}
	// make iteration deterministic
	var keys []int
	for k := range s2g {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	// find GID with maximum shards
	index, max := -1, -1
	for _, gid := range keys {
		if len(s2g[gid]) > max {
			index, max = gid, len(s2g[gid])
		}
	}
	return index
}

func deepCopy(groups map[int][]string) map[int][]string {
	newGroups := make(map[int][]string)
	for gid, servers := range groups {
		newServers := make([]string, len(servers))
		copy(newServers, servers)
		newGroups[gid] = newServers
	}
	return newGroups
}
