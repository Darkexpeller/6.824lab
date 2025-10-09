package main

//
// start the coordinator process, which is implemented
// in ../mr/coordinator.go
//
// go run mrcoordinator.go pg*.txt
//
// Please do not change this file.
//

import (
	"fmt"
	"log"
	"net/rpc"
	"os"
	"time"

	"6.5840/mr"
)

func exa(m *mr.Coordinator) {
	client, err := rpc.DialHTTP("unix", m.GetSock())
	if err != nil {
		log.Fatal("Dialing error:", err)
	}
	args := mr.ExampleArgs{}
	args.X = 1
	var reply mr.ExampleReply
	reply.Y = 3
	err = client.Call("Coordinator.Example", &args, &reply)
	if err != nil {
		log.Fatal("RPC call error:", err)
	}

	fmt.Println(reply.Y)
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
		os.Exit(1)
	}

	m := mr.MakeCoordinator(os.Args[1:], 10)
	//exa(m)
	for m.Done() == false {
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)
}
