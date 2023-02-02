package mr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sync"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use iHash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func iHash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapF func(string, string) []KeyValue,
	reduceF func(string, []string) string) {
	// 1.  传入map函数、reduce函数
	// 2. 循环，使用工作的心跳，得到心跳响应
	// 3. 如果心跳响应的任务类型是map，使用工作的执行map
	// 4. 如果心跳响应的任务类型是reduce，使用工作的执行reduce
	// 5. 如果心跳响应的任务类型是等待，sleep 1s
	// 6. 如果心跳响应的任务类型是完成，结束

	for {
		response := doHeartbeat()
		log.Printf("Worker: receive coordinator's heartbeat %v \n", response)
		switch response.JobType {
		case MapJob:
			doMapTask(mapF, response)
		case ReduceJob:
			doReduceTask(reduceF, response)
		case WaitJob:
			time.Sleep(1 * time.Second)
		case CompleteJob:
			return
		default:
			panic(fmt.Sprintf("unexpected jobType %v", response.JobType))
		}
	}
}

func doMapTask(mapF func(string, string) []KeyValue, response *HeartbeatResponse) {
	// 1. 传入map函数、心跳响应
	// 2. 用心跳响应的文件路径打开文件，读取文件内容
	// 3. 调用map函数，传入文件路径、文件内容，传出kv数组
	// 4. 遍历kv数组，将k哈希得到h，取余得到索引，将索引相同的kv放到同一个数组，得到中间数据，中间数据有多个kv数组
	// 5. 遍历中间数据每一个kv数组
	// 6. 对于每个kv数组，创建协程
	// 7. 使用心跳响应的编号和索引，获得文件路径
	// 8. 使用json的newencoder创建json对象
	// 9. 对kv数组的每个kv，使用encode保存到json对象
	// 10. 使用原子写入文件，传入路径和join对象的数据
	// 11. 使用sync的waitgroup，等待所有协程完毕
	// 12. 使用工作的完成，传入心跳响应编号、map阶段

	fileName := response.FilePath
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()
	kva := mapF(fileName, string(content))
	intermediates := make([][]KeyValue, response.NReduce)
	for _, kv := range kva {
		index := iHash(kv.Key) % response.NReduce
		intermediates[index] = append(intermediates[index], kv)
	}
	var wg sync.WaitGroup
	for index, intermediate := range intermediates {
		wg.Add(1)
		go func(index int, intermediate []KeyValue) {
			defer wg.Done()
			intermediateFilePath := generateMapResultFileName(response.Id, index)
			var buf bytes.Buffer
			enc := json.NewEncoder(&buf)
			for _, kv := range intermediate {
				err := enc.Encode(&kv)
				if err != nil {
					log.Fatalf("cannot encode json %v", kv.Key)
				}
			}
			atomicWriteFile(intermediateFilePath, &buf)
		}(index, intermediate)
	}
	wg.Wait()
	doReport(response.Id, MapPhase)
}

func doReduceTask(reduceF func(string, []string) string, response *HeartbeatResponse) {
	// 1. 传入reduce函数、心跳响应
	// 2. 遍历map的索引
	// 3. 根据索引和心跳响应的编号，获得文件路径
	// 4. 打开文件
	// 5. 创建文件对象
	// 6. 循环，使用decode从文件对象的数据获取kv，附加到kv数组
	// 7. 遍历kv数组，对于遍历kv，保存到kv哈希表
	// 8. 遍历kv哈希表，对于遍历kv，调用reduce函数，传入kv，传出结果
	// 9. 将结果附加到最终结果
	// 10. 使用原子写入文件，传入文件路径和最终结果
	// 11. 使用工作的完成，传入心跳响应编号，reduce阶段

	var kva []KeyValue
	for i := 0; i < response.NMap; i++ {
		filePath := generateMapResultFileName(i, response.Id)
		file, err := os.Open(filePath)
		if err != nil {
			log.Fatalf("cannot open %v", filePath)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	results := make(map[string][]string)
	// Maybe we need merge sort for larger data
	for _, kv := range kva {
		results[kv.Key] = append(results[kv.Key], kv.Value)
	}
	var buf bytes.Buffer
	for key, values := range results {
		output := reduceF(key, values)
		fmt.Fprintf(&buf, "%v %v\n", key, output)
	}
	atomicWriteFile(generateReduceResultFileName(response.Id), &buf)
	doReport(response.Id, ReducePhase)
}

func doHeartbeat() *HeartbeatResponse {
	// 1. 传出心跳响应
	// 2. 构造心跳请求，心跳响应
	// 3. call，传入心跳请求，心跳响应

	response := HeartbeatResponse{}
	call("Coordinator.Heartbeat", &HeartbeatRequest{}, &response)
	return &response
}

func doReport(id int, phase SchedulePhase) {
	// 1. 传入编号，阶段
	// 2. 构造完成请求，完成响应
	// 3. call，传入完成请求，完成响应

	call("Coordinator.Report", &ReportRequest{id, phase}, &ReportResponse{})
}

func call(rpcName string, args interface{}, reply interface{}) bool {
	// 1. 传入rpc名、请求，传出响应
	// 2. 使用rpc的dialhttp，传入unix和地址，传出连接对象
	// 3. 使用rpc的call，传入rpc名，请求、传出响应

	sockName := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcName, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
