package mr

import (
	"container/list"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

func InitCoorLog() int {
	logFile, err := os.OpenFile("../Coordinator.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return -1
	}
	log.SetOutput(logFile)
	log.SetFlags(log.Llongfile | log.Lmicroseconds | log.Ldate)
	return 1
}

// 任务队列
type Queue struct {
	mu    sync.Mutex
	items *list.List
}

// 创建队列
func NewQueue() *Queue {
	return &Queue{items: list.New()}
}

// 入队
func (q *Queue) enqueueNoLock(task RpcTask) {
	q.items.PushBack(task)
}

func (q *Queue) Enqueue(task RpcTask) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.enqueueNoLock(task)
}

// 出队
func (q *Queue) dequeueNoLock() RpcTask {
	if q.items.Len() == 0 {
		panic("Queue is empty")
	}
	element := q.items.Front()
	q.items.Remove(element)
	return element.Value.(RpcTask)
}

func (q *Queue) Dequeue() RpcTask {
	q.mu.Lock()
	defer q.mu.Unlock()

	return q.dequeueNoLock()
}

// 判断队列是否为空
func (q *Queue) IsEmpty() bool {
	return q.items.Len() == 0
}

// 获取队列长度
func (q *Queue) Size() int {
	return q.items.Len()
}

// 删除指定元素
func (q *Queue) deleteNoLock(Tasknumber int) bool {
	e := q.items.Front()
	for ; e != nil && e.Value.(RpcTask).Tasknumber != Tasknumber; e = e.Next() {

	}
	if e == nil {
		panic("no this TaskNumber")
		return false
	}
	q.items.Remove(e)
	return true
}
func (q *Queue) Delete(Tasknumber int) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.deleteNoLock(Tasknumber)
}

func (q *Queue) findByTaskNumberNoLock(Tasknumber int) (RpcTask, error) {
	e := q.items.Front()
	for ; e != nil && e.Value.(RpcTask).Tasknumber != Tasknumber; e = e.Next() {

	}
	if e == nil {
		//log.Printf("no this TaskNumber")
		return RpcTask{}, fmt.Errorf("task %d not found", Tasknumber)
	}
	return e.Value.(RpcTask), nil
}
func (q *Queue) FindByTaskNumber(Tasknumber int) (RpcTask, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	return q.findByTaskNumberNoLock(Tasknumber)
}

type Coordinator struct {
	TaskQueue    *Queue //待执行任务
	RunningTask  *Queue //进行中任务
	nReduce      int    // nReduce is the number of reduce tasks to use
	nMap         int
	Tasknumber   int //任务数量
	TaskStage    int //目前所处任务状态
	nextWorkerID int
	TempFile     []string //记录中间文件位置，采用json存储
}

// 处理任务
func (c *Coordinator) RpcHandle(request *RpcRequest, task *RpcTask) error {

	if request == nil {
		return nil
	}
	switch request.MesType {
	case INIT: //初始化请求,返回Map和Reduce的任务数量
		task.TaskType = INIT_TASK
		task.NMap = c.nMap
		task.NReduce = c.nReduce
		task.Tasknumber = c.assignWorkerID()
		log.Printf("Have INIT Rpc  task.nMap=%d, task.nReduce=%d task.Tasknumber(WorkerID)=%d ", task.NMap, task.NReduce, task.Tasknumber)
		return nil

	case REQUEST: //请求任务

		if c.TaskQueue.IsEmpty() {
			task.TaskType = NULL_TASK
			return nil
		}
		*task = c.TaskQueue.Dequeue()
		task.NMap = c.nMap
		task.BeginTime = time.Now().Unix()
		task.TaskStatus = HAD_RUN
		task.WorkerID = request.WorkerID
		log.Printf("Have REQUEST Rpc %v ", *task)
		c.RunningTask.Enqueue(*task)
		return nil

	case RESPONSE: //响应任务状态请求
		switch request.TaskStatus {
		case TASK_SUCCEED:
			runtask, err := c.RunningTask.FindByTaskNumber(request.Tasknumber)
			if err == nil && runtask.WorkerID == request.WorkerID {
				c.RunningTask.Delete(request.Tasknumber)
			} else {
				task.TaskType = INVALID_TASK
				log.Printf("WorkerID no same :request.WorkerID=%d request.Tasknumber=%d error:%v", request.WorkerID, request.Tasknumber, err)
			}

			return nil
		case TASK_FAILED:
			e, err := c.RunningTask.FindByTaskNumber(request.Tasknumber)
			if err != nil {
				task.TaskType = INVALID_TASK
				return nil
			}
			e.TaskStatus = NO_RUN
			c.TaskQueue.Enqueue(e)
			c.RunningTask.Delete(request.Tasknumber)
			return nil
		case HAD_RUN:
			task.TaskType = NO_RUN
			return nil
		default:
			break
		}
	}

	return nil
}

//分配WorkerID

func (c *Coordinator) assignWorkerID() int {
	id := c.nextWorkerID
	c.nextWorkerID++
	return id
}

// 负责检测任务完成状态
func (c *Coordinator) CheckWorkStatus(flag uint) bool {
	time.Sleep(100 * time.Millisecond) // 延迟0.1秒
	if c.TaskStage == int(flag) && c.TaskQueue.IsEmpty() && c.RunningTask.IsEmpty() {
		return false
	}
	return true

}

func (c *Coordinator) taskNumberalloc() int {
	l := c.Tasknumber
	c.Tasknumber = c.Tasknumber + 1
	return l
}

func (c *Coordinator) init(files []string) {
	c.TaskStage = MAP_STAGE
	for file := range files { //每一个file都要配置一个Map任务
		t := RpcTask{MAP_TASK, files[file], c.taskNumberalloc(), NO_RUN, 0, c.nReduce, 0, 0}
		c.nMap = c.nMap + 1
		c.TaskQueue.Enqueue(t)
	}
	// log.Printf("TaskQueue : %d nMap:%d nReduce:%d", c.TaskQueue.Size(), c.nMap, c.nReduce)
}

// 对Running队列进行轮询，对于超过10s的任务进行重新分配任务
// 任务完成检测
func (c *Coordinator) beginMapStage() bool {
	ret := true
	log.Printf("beginMapStage")
	for c.CheckWorkStatus(MAP_STAGE) {
		time.Sleep(100 * time.Millisecond)
		c.RunningTask.mu.Lock()

		for e := c.RunningTask.items.Front(); e != nil; e = e.Next() {
			task := e.Value.(RpcTask)
			if (time.Now().Unix() - task.BeginTime) >= 10 {
				log.Printf("任务超时: Tasknumber=%d 所属Woker:%d", task.Tasknumber, task.WorkerID)
				c.TaskQueue.Enqueue(task)
				c.RunningTask.deleteNoLock(task.Tasknumber)
			}
		}
		c.RunningTask.mu.Unlock()
	}
	return ret
}

// 开始Reduce阶段处理
func (c *Coordinator) beginReduceStage() bool {
	ret := true
	log.Printf("beginReduceStage")
	for i := 0; i < c.nReduce; i = i + 1 {
		t := RpcTask{REDUCE_TASK, "", i, NO_RUN, 0, c.nReduce, 0, 0}
		c.TaskQueue.Enqueue(t)
	}

	c.TaskStage = REDUCE_STAGE

	for c.CheckWorkStatus(REDUCE_STAGE) {
		time.Sleep(100 * time.Millisecond)
		c.RunningTask.mu.Lock()

		for e := c.RunningTask.items.Front(); e != nil; e = e.Next() {
			task := e.Value.(RpcTask)
			if (time.Now().Unix() - task.BeginTime) >= 10 {
				c.TaskQueue.Enqueue(task)
				c.RunningTask.deleteNoLock(task.Tasknumber)
			}
		}
		c.RunningTask.mu.Unlock()
	}

	return ret
}

func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":8001")
	sockname := coordinatorSock() //可以考虑使用Abstract Namespace，不涉及内存IO
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	InitCoorLog()
	ret := true
	log.Printf("begin Done()")
	if !c.beginMapStage() {
		return false
	}
	if !c.beginReduceStage() {
		return false
	}
	// Your code here.
	log.Printf("end Done()")
	return ret
}
func (c *Coordinator) GetSock() string {
	return coordinatorSock()
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{nReduce: nReduce, TaskQueue: NewQueue(), RunningTask: NewQueue(), nMap: 0}
	c.init(files)
	c.server()
	return &c
}
