package kvsrv

import (
	"log"
	"net"
	"os"

	netrpc "net/rpc"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

func InitCoorLog() int {
	logFile, err := os.OpenFile("../Server.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return -1
	}
	log.SetOutput(logFile)
	log.SetFlags(log.Llongfile | log.Lmicroseconds | log.Ldate)
	return 1
}

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type VV struct { //(value,version)
	value   string
	version uint64
}

type KVServer struct {
	mu   sync.Mutex
	data map[string]VV
	// Your definitions here.
}

func MakeKVServer() *KVServer {

	kv := &KVServer{}
	rpcs := netrpc.NewServer()
	rpcs.Register(kv)
	l, e := net.Listen("tcp", "1234")
	if e != nil {
		log.Printf("网络出错")
		return nil
	}
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				go rpcs.ServeConn(conn)

			} else {
				break
			}
		}
		l.Close()
	}()

	// Your code here.
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// Args.Version is 0.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	InitCoorLog()
	kv := MakeKVServer()
	return []tester.IService{kv}
}
