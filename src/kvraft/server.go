package kvraft

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type KVServer struct {
	mu      sync.RWMutex
	dead    int32
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxRaftState int // snapshot if log grows this big
	lastApplied  int // record the lastApplied to prevent stateMachine from rollback

	stateMachine   KVStateMachine                // KV stateMachine
	lastOperations map[int64]OperationContext    // determine whether log is duplicated by recording the last commandId and response corresponding to the clientId
	notifyChans    map[int]chan *CommandResponse // notify client goroutine by applier goroutine to response
}

func (kv *KVServer) Command(request *CommandRequest, response *CommandResponse) {
	// 1. 传入命令请求，传出命令响应
	// 2. 如果命令请求的命令类型不是get，且命令请求重复，直接返回
	// 3. 使用raft的开始，传入命令请求，传出新日志索引、标记是否领导者
	// 4. 如果标记不是领导者，直接返回
	// 5. 利用新日志索引从命令响应管道数组找到命令响应管道
	// 6. select监视命令响应管道和超时管道
	// 7. 如果命令响应管道，得到命令响应
	// 8. 如果超时管道，设置命令响应的错误
	// 9. 创建协程，删除命令响应管道数组的命令响应管道。

	defer DPrintf("{Node %v} processes CommandRequest %v with CommandResponse %v", kv.rf.Me(), request, response)
	// return result directly without raft layer's participation if request is duplicated
	kv.mu.RLock()
	if request.Op != OpGet && kv.isDuplicateRequest(request.ClientId, request.CommandId) {
		lastResponse := kv.lastOperations[request.ClientId].LastResponse
		response.Value, response.Err = lastResponse.Value, lastResponse.Err
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()
	// do not hold lock to improve throughput
	// when KVServer holds the lock to take snapshot, underlying raft can still commit raft logs
	index, _, isLeader := kv.rf.Start(Command{request})
	if !isLeader {
		response.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	ch := kv.getNotifyChan(index)
	kv.mu.Unlock()
	select {
	case result := <-ch:
		response.Value, response.Err = result.Value, result.Err
	case <-time.After(ExecuteTimeout):
		response.Err = ErrTimeout
	}
	// release notifyChan to reduce memory footprint
	// why asynchronously? to improve throughput, here is no need to block client request
	go func() {
		kv.mu.Lock()
		kv.removeOutdatedNotifyChan(index)
		kv.mu.Unlock()
	}()
}

// each RPC imply that the client has seen the reply for its previous RPC
// therefore, we only need to determine whether the latest commandId of a clientId meets the criteria
func (kv *KVServer) isDuplicateRequest(clientId int64, requestId int64) bool {
	operationContext, ok := kv.lastOperations[clientId]
	return ok && requestId <= operationContext.MaxAppliedCommandId
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	DPrintf("{Node %v} has been killed", kv.rf.Me())
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *KVServer) killed() bool {
	return atomic.LoadInt32(&kv.dead) == 1
}

// a dedicated applier goroutine to apply committed entries to stateMachine, take snapshot and apply snapshot from raft
func (kv *KVServer) applier() {
	// 1. 循环select监听应用消息管道
	// 2. 如果应用消息管道，得到应用消息
	// 3. 如果应用消息的标记命令有效
	// 4. 如果应用消息的命令索引小于等于最后一次应用的命令索引，continue
	// 5. 对于应用消息的命令请求，如果命令请求的请求类型是不get，并且命令请求重复，则命令响应？
	// 6. 使用kv服务器的应用日志到状态机，传入命令请求，传出命令响应
	// 7. 如果命令请求的命令类型不是get，更新最后操作数组？
	// 8. 使用raft的获得状态，传出当前任期和标记是否领导者，如果标记是领导者且命令请求的命令任期等于当前任期，利用命令请求的命令索引从命令响应管道数组找到命令响应管道，将命令响应交给命令响应管道。
	// 9. ？快照
	// 10. 如果应用消息的标记命令无效，但应用消息的标记快照有效？

	for kv.killed() == false {
		select {
		case message := <-kv.applyCh:
			DPrintf("{Node %v} tries to apply message %v", kv.rf.Me(), message)
			if message.CommandValid {
				kv.mu.Lock()
				if message.CommandIndex <= kv.lastApplied {
					DPrintf("{Node %v} discards outdated message %v because a newer snapshot which lastApplied is %v has been restored", kv.rf.Me(), message, kv.lastApplied)
					kv.mu.Unlock()
					continue
				}
				kv.lastApplied = message.CommandIndex

				var response *CommandResponse
				command := message.Command.(Command)
				if command.Op != OpGet && kv.isDuplicateRequest(command.ClientId, command.CommandId) {
					DPrintf("{Node %v} doesn't apply duplicated message %v to stateMachine because maxAppliedCommandId is %v for client %v", kv.rf.Me(), message, kv.lastOperations[command.ClientId], command.ClientId)
					response = kv.lastOperations[command.ClientId].LastResponse
				} else {
					response = kv.applyLogToStateMachine(command)
					if command.Op != OpGet {
						kv.lastOperations[command.ClientId] = OperationContext{command.CommandId, response}
					}
				}

				// only notify related channel for currentTerm's log when node is leader
				if currentTerm, isLeader := kv.rf.GetState(); isLeader && message.CommandTerm == currentTerm {
					ch := kv.getNotifyChan(message.CommandIndex)
					ch <- response
				}

				needSnapshot := kv.needSnapshot()
				if needSnapshot {
					kv.takeSnapshot(message.CommandIndex)
				}
				kv.mu.Unlock()
			} else if message.SnapshotValid {
				kv.mu.Lock()
				if kv.rf.CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot) {
					kv.restoreSnapshot(message.Snapshot)
					kv.lastApplied = message.SnapshotIndex
				}
				kv.mu.Unlock()
			} else {
				panic(fmt.Sprintf("unexpected Message %v", message))
			}
		}
	}
}

func (kv *KVServer) needSnapshot() bool {
	return kv.maxRaftState != -1 && kv.rf.GetRaftStateSize() >= kv.maxRaftState
}

func (kv *KVServer) takeSnapshot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.stateMachine)
	e.Encode(kv.lastOperations)
	kv.rf.Snapshot(index, w.Bytes())
}

func (kv *KVServer) restoreSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) == 0 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var stateMachine MemoryKV
	var lastOperations map[int64]OperationContext
	if d.Decode(&stateMachine) != nil ||
		d.Decode(&lastOperations) != nil {
		DPrintf("{Node %v} restores snapshot failed", kv.rf.Me())
	}
	kv.stateMachine, kv.lastOperations = &stateMachine, lastOperations
}

func (kv *KVServer) getNotifyChan(index int) chan *CommandResponse {
	if _, ok := kv.notifyChans[index]; !ok {
		kv.notifyChans[index] = make(chan *CommandResponse, 1)
	}
	return kv.notifyChans[index]
}

func (kv *KVServer) removeOutdatedNotifyChan(index int) {
	delete(kv.notifyChans, index)
}

func (kv *KVServer) applyLogToStateMachine(command Command) *CommandResponse {
	// 1. 传入命令请求，传出命令响应
	// 2. 根据命令请求的命令类型，分别使用kv状态机的get、put、append，传入命令请求的kv

	var value string
	var err Err
	switch command.Op {
	case OpPut:
		err = kv.stateMachine.Put(command.Key, command.Value)
	case OpAppend:
		err = kv.stateMachine.Append(command.Key, command.Value)
	case OpGet:
		value, err = kv.stateMachine.Get(command.Key)
	}
	return &CommandResponse{err, value}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// 1. 创建协程使用kv服务器的引用

	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Command{})
	applyCh := make(chan raft.ApplyMsg)

	kv := &KVServer{
		maxRaftState:   maxraftstate,
		applyCh:        applyCh,
		dead:           0,
		lastApplied:    0,
		rf:             raft.Make(servers, me, persister, applyCh),
		stateMachine:   NewMemoryKV(),
		lastOperations: make(map[int64]OperationContext),
		notifyChans:    make(map[int]chan *CommandResponse),
	}
	kv.restoreSnapshot(persister.ReadSnapshot())
	// start applier goroutine to apply committed logs to stateMachine
	go kv.applier()

	DPrintf("{Node %v} has started", kv.rf.Me())
	return kv
}
