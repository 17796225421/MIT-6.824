package shardctrler

import (
	"6.824/raft"
	"fmt"
	"sync/atomic"
	"time"
)
import "6.824/labrpc"
import "sync"
import "6.824/labgob"

type ShardCtrler struct {
	mu      sync.RWMutex
	dead    int32
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	stateMachine   ConfigStateMachine            // Config stateMachine
	lastOperations map[int64]OperationContext    // determine whether log is duplicated by recording the last commandId and response corresponding to the clientId
	notifyChans    map[int]chan *CommandResponse // notify client goroutine by applier goroutine to response
}

func (sc *ShardCtrler) Command(request *CommandRequest, response *CommandResponse) {
	// 1. 传入命令请求，传出命令响应
	// 2. 如果命令请求的命令类型不是query，且命令请求重复，直接返回
	// 3. 使用raft的开始，传入命令请求，传出新日志索引、标记是否领导者
	// 4. 如果标记不是领导者，直接返回
	// 5. 利用新日志索引从命令响应管道数组找到命令响应管道
	// 6. select监视命令响应管道和超时管道
	// 7. 如果命令响应管道，得到命令响应
	// 8. 如果超时管道，设置命令响应的错误
	// 9. 创建协程，删除命令响应管道数组的命令响应管道

	defer DPrintf("{Node %v}'s state is {}, processes CommandRequest %v with CommandResponse %v", sc.rf.Me(), request, response)
	// return result directly without raft layer's participation if request is duplicated
	sc.mu.RLock()
	if request.Op != OpQuery && sc.isDuplicateRequest(request.ClientId, request.CommandId) {
		lastResponse := sc.lastOperations[request.ClientId].LastResponse
		response.Config, response.Err = lastResponse.Config, lastResponse.Err
		sc.mu.RUnlock()
		return
	}
	sc.mu.RUnlock()
	// do not hold lock to improve throughput
	index, _, isLeader := sc.rf.Start(Command{request})
	if !isLeader {
		response.Err = ErrWrongLeader
		return
	}
	sc.mu.Lock()
	ch := sc.getNotifyChan(index)
	sc.mu.Unlock()
	select {
	case result := <-ch:
		response.Config, response.Err = result.Config, result.Err
	case <-time.After(ExecuteTimeout):
		response.Err = ErrTimeout
	}
	// release notifyChan to reduce memory footprint
	// why asynchronously? to improve throughput, here is no need to block client request
	go func() {
		sc.mu.Lock()
		sc.removeOutdatedNotifyChan(index)
		sc.mu.Unlock()
	}()
}

// each RPC imply that the client has seen the reply for its previous RPC
// therefore, we only need to determine whether the latest commandId of a clientId meets the criteria
func (sc *ShardCtrler) isDuplicateRequest(clientId int64, requestId int64) bool {
	operationContext, ok := sc.lastOperations[clientId]
	return ok && requestId <= operationContext.MaxAppliedCommandId
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	DPrintf("{ShardCtrler %v} has been killed", sc.rf.Me())
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
}

func (sc *ShardCtrler) killed() bool {
	return atomic.LoadInt32(&sc.dead) == 1
}

func (sc *ShardCtrler) getNotifyChan(index int) chan *CommandResponse {
	if _, ok := sc.notifyChans[index]; !ok {
		sc.notifyChans[index] = make(chan *CommandResponse, 1)
	}
	return sc.notifyChans[index]
}

func (sc *ShardCtrler) removeOutdatedNotifyChan(index int) {
	delete(sc.notifyChans, index)
}

func (sc *ShardCtrler) applyLogToStateMachine(command Command) *CommandResponse {
	var config Config
	var err Err
	switch command.Op {
	case OpJoin:
		err = sc.stateMachine.Join(command.Servers)
	case OpLeave:
		err = sc.stateMachine.Leave(command.GIDs)
	case OpMove:
		err = sc.stateMachine.Move(command.Shard, command.GID)
	case OpQuery:
		config, err = sc.stateMachine.Query(command.Num)
	}
	return &CommandResponse{err, config}
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// a dedicated applier goroutine to apply committed entries to stateMachine
func (sc *ShardCtrler) applier() {
	// 1. 循环select监听应用消息管道
	// 2. 如果应用消息管道，得到应用消息
	// 3. 如果应用消息的标记命令有效
	// 4. 从应用消息取出命令请求
	// 5. 如果命令请求的命令类型不是query，且命令请求重复，从？得到命令响应
	// 6. 否则使用配置管理服务器的应用日志到状态机，传入命令请求，传出命令响应
	// 7. 使用raft的获得状态，传出当前任期和标记是否领导者，如果标记是领导者且命令请求的命令任期等于当前任期，利用命令请求的命令索引从命令响应管道数组找到命令响应管道，将命令响应交给命令响应管道。

	for sc.killed() == false {
		select {
		case message := <-sc.applyCh:
			DPrintf("{Node %v} tries to apply message %v", sc.rf.Me(), message)
			if message.CommandValid {
				var response *CommandResponse
				command := message.Command.(Command)
				sc.mu.Lock()

				if command.Op != OpQuery && sc.isDuplicateRequest(command.ClientId, command.CommandId) {
					DPrintf("{Node %v} doesn't apply duplicated message %v to stateMachine because maxAppliedCommandId is %v for client %v", sc.rf.Me(), message, sc.lastOperations[command.ClientId], command.ClientId)
					response = sc.lastOperations[command.ClientId].LastResponse
				} else {
					response = sc.applyLogToStateMachine(command)
					if command.Op != OpQuery {
						sc.lastOperations[command.ClientId] = OperationContext{command.CommandId, response}
					}
				}

				// only notify related channel for currentTerm's log when node is leader
				if currentTerm, isLeader := sc.rf.GetState(); isLeader && message.CommandTerm == currentTerm {
					ch := sc.getNotifyChan(message.CommandIndex)
					ch <- response
				}

				sc.mu.Unlock()
			} else {
				panic(fmt.Sprintf("unexpected Message %v", message))
			}
		}
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Command{})
	applyCh := make(chan raft.ApplyMsg)

	sc := &ShardCtrler{
		applyCh:        applyCh,
		dead:           0,
		rf:             raft.Make(servers, me, persister, applyCh),
		stateMachine:   NewMemoryConfigStateMachine(),
		lastOperations: make(map[int64]OperationContext),
		notifyChans:    make(map[int]chan *CommandResponse),
	}
	// start applier goroutine to apply committed logs to stateMachine
	go sc.applier()

	DPrintf("{ShardCtrler %v} has started", sc.rf.Me())
	return sc
}
