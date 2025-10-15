package lock

import (
	"fmt"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type LockState string

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interfaces hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck      kvtest.IKVClerk
	ls      string
	version rpc.Tversion
	id      string
	// You may add code here
}

// The tester calls MakeLock() and passes in a k/v clerk; you code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck, ls: l, id: kvtest.RandValue(8)}
	ck.Put(l, "", 0)
	// You may add code here
	return lk
}

func (lk *Lock) Acquire() {
	for {

		value, version, _ := lk.ck.Get(lk.ls)
		if value == "" { //锁处于空闲状态
			Err := lk.ck.Put(lk.ls, lk.id, version)
			if Err == rpc.OK {
				lk.version = version + 1
				return
			}
		} else {
			continue
		}

	}
	// Your code here
}

func (lk *Lock) Release() {
	Err := lk.ck.Put(lk.ls, "", lk.version)
	if Err != rpc.OK {
		fmt.Printf("锁释放失败:%d", lk.version)
	}
	// Your code here
}
