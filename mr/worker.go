package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

func InitWorkerLog() int {
	logFile, err := os.OpenFile("../Worker.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return -1
	}
	log.SetOutput(logFile)
	log.SetFlags(log.Llongfile | log.Lmicroseconds | log.Ldate)
	return 1
}

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}
type WorkerHandler struct {
	mapf     func(string, string) []KeyValue
	reducef  func(string, []string) string
	client   *rpc.Client
	nMap     int
	nReduce  int
	pid      int
	WorkerID int
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	InitWorkerLog()
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)

	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()
	w := NewWorkerHandler(mapf, reducef, c)
	if w == nil {
		log.Printf("Worker PID:%d: the worker exit ", os.Getpid())
		return
	}
	if w.WorkLoop() {

	}
	log.Printf("Worker PID:%d: the worker exit ", w.pid)
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func NewWorkerHandler(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string, client *rpc.Client) *WorkerHandler {

	w := &WorkerHandler{mapf, reducef, client, 0, 0, 0, 0}
	if !w.initWorker() {
		return nil
	}
	log.Printf("Worker ID:%d: init succeed", w.WorkerID)
	return w
}

func (worker *WorkerHandler) initWorker() bool {
	for {
		time.Sleep(100 * time.Millisecond)
		init_task, err := worker.acquireTask(RpcRequest{INIT, 0, 0, 0})
		if err != nil {
			log.Fatalf("initWorker failed")
			return false
		}
		if init_task.TaskType == INIT_TASK {
			worker.nMap = init_task.NMap
			worker.nReduce = init_task.NReduce
			worker.pid = os.Getpid()
			worker.WorkerID = init_task.Tasknumber //Tasknumber字段在初始化时用来传递WorkerID
			return true
		}
	}

}

func (worker *WorkerHandler) acquireTask(request RpcRequest) (RpcTask, error) {

	task := RpcTask{}

	err := worker.client.Call("Coordinator.RpcHandle", &request, &task)
	if err != nil {
		log.Printf("WorkerID %d: Rpc failed", worker.WorkerID)
		return RpcTask{}, err
	}
	return task, nil
}

func (worker *WorkerHandler) applyTask(Tasknumber int) bool {
	request := RpcRequest{RESPONSE, TASK_SUCCEED, Tasknumber, worker.WorkerID}
	task := RpcTask{}
	err := worker.client.Call("Coordinator.RpcHandle", &request, &task)
	if err != nil {
		log.Printf("WorkerID %d: Rpc failed", worker.WorkerID)
		return false
	}
	switch task.TaskType {
	case INVALID_TASK:
		return false
	default:
		return true
	}
	return true
}

func (worker *WorkerHandler) WorkLoop() bool {
	for {
		time.Sleep(1000 * time.Millisecond)
		task, err := worker.acquireTask(RpcRequest{REQUEST, 0, 0, worker.WorkerID})
		if err != nil {
			log.Fatalf("WorkerID %d: WorkLoop failed", worker.WorkerID)
			return false
		}
		switch task.TaskType {
		case MAP_TASK:
			worker.handleMapTask(task)

			continue
		case REDUCE_TASK:
			worker.handleReduceTask(task)

			continue
		case NULL_TASK:
			continue
		case EXIT_TASK:
			return true
		default:
			continue
		}
	}

}

func (worker *WorkerHandler) handleMapTask(task RpcTask) error {
	file, err := os.Open(task.Filename)
	if err != nil {
		log.Fatalf("cannot open %v", task.Filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.Filename)
	}
	file.Close()
	kva := worker.mapf(task.Filename, string(content))

	buckets := make([][]KeyValue, task.NReduce)
	for _, kv := range kva {
		r := ihash(kv.Key) % task.NReduce
		buckets[r] = append(buckets[r], kv)
	}
	if !worker.applyTask(task.Tasknumber) {
		log.Printf("the task is invalid")
		return fmt.Errorf("the task is invalid")

	}
	for i := 0; i < task.NReduce; i = i + 1 {
		// filename := "mr-" + strconv.FormatInt(int64(task.Tasknumber), 10) + "-" + strconv.FormatInt(int64(i), 10) + ".json"
		filename := fmt.Sprintf("mr-%d-%d.json", task.Tasknumber, i)
		file, err := os.Create(filename)

		if err != nil {
			log.Printf("cannot open %v", filename)
			defer file.Close()
			return fmt.Errorf("cannot open %v", filename)
		} else {
			defer file.Close()
			encoder := json.NewEncoder(file)
			err = encoder.Encode(buckets[i])
			if err != nil {
				log.Printf("写入 JSON 失败 %s: %v", filename, err)
				return fmt.Errorf("写入 JSON 失败 %s: %v", filename, err)
			}
		}

	}

	return nil
}

// 读取 mr-X-Y 格式的中间文件
func (worker *WorkerHandler) readIntermediateFiles(task RpcTask) ([]KeyValue, error) {
	var kva []KeyValue

	for m := 0; m < worker.nMap; m++ {
		filename := fmt.Sprintf("mr-%d-%d.json", m, task.Tasknumber)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
			return kva, fmt.Errorf("open failed")
		}
		defer file.Close()
		var intermediate []KeyValue
		decoder := json.NewDecoder(file)
		if err := decoder.Decode(&intermediate); err != nil {
			return nil, fmt.Errorf("解析文件 %s 失败: %v", filename, err)
		}
		log.Printf("WorkerID %d: Reduce intermediate=%v  tasknumber=%d", worker.WorkerID, intermediate, task.Tasknumber)
		kva = append(kva, intermediate...)
	}
	return kva, nil
}

func (worker *WorkerHandler) handleReduceTask(task RpcTask) error {
	kva, err := worker.readIntermediateFiles(task)
	if err != nil {
		return fmt.Errorf("ReduceTask failed: %v", err)
	}
	sort.Sort(ByKey(kva))
	tempfilename := fmt.Sprintf("mr-out-temp-%d", task.Tasknumber)
	tempfile, _ := os.Create(tempfilename)

	for i := 0; i < len(kva); {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := worker.reducef(kva[i].Key, values)

		fmt.Fprintf(tempfile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	if !worker.applyTask(task.Tasknumber) {
		return fmt.Errorf("the task is invalid")

	}
	if os.Rename(tempfilename, fmt.Sprintf("mr-out-%d", task.Tasknumber)) != nil {
		log.Printf("temp file Rename failed")
		return fmt.Errorf("temp file Rename failed")
	}

	return nil

}
