package mr

//
// RPC definitions.
//

// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type RpcRequest struct {
	MesType    int //响应类型
	TaskStatus int //响应任务完成状态
	Tasknumber int //响应任务号
	WorkerID   int
}

type RpcTask struct {
	TaskType   int    //任务类型
	Filename   string //Map阶段用，指定处理的文件名
	Tasknumber int    //Map和Reduce的任务号
	TaskStatus int    //任务状态
	BeginTime  int64  //任务开始时间，从任务进入到RunningTask队列时开始计时
	NReduce    int    //Reduce任务数
	NMap       int    //Map任务数
	WorkerID   int    //任务归属的WorkerID
}

const (
	//MesType,作为Worker向Coordinator发送的RpcRequest类型
	REQUEST = iota
	RESPONSE
	INIT

	//RpcRequest.TaskStatus
	TASK_SUCCEED
	TASK_FAILED

	//RpcTask.TaskStatus标识任务状态
	IN_RUN
	NO_RUN
	HAD_RUN

	//RpcTask.TaskType,标识任务名称
	MAP_TASK     //Map任务
	REDUCE_TASK  //Reuduce任务
	INVALID_TASK //无效任务，代表当前Worker不拥有该任务提交权
	NULL_TASK    //空任务
	EXIT_TASK    //种植任务
	INIT_TASK    //初始化任务，Worker向Coordinator发送初始化请求时分配的任务类型

	//Coordinator.TaskStage标识当前所处的任务阶段
	MAP_STAGE
	REDUCE_STAGE
	END_STAGE
)

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
