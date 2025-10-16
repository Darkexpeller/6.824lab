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
	fmt.Printf("id: %v\n", lk.id)
	var err rpc.Err
	for {
		err = ck.Put(l, "", 0)
		if err != rpc.ErrMaybe {
			break
		}
	}

	// You may add code here
	return lk
}

func (lk *Lock) Acquire() {
	for {

		value, version, _ := lk.ck.Get(lk.ls)
		if value == "" { //锁处于空闲状态

			Err := lk.ck.Put(lk.ls, lk.id, version) //即使有多个client同时感知到空闲锁，由于(value,version)的存在且Put操作并发安全，因此只允许一个client获取到锁
			if Err == rpc.OK {
				lk.version = version + 1
				return
			}
		} else if value == lk.id { //ErrMaybe情况，Put()可能已经把值应用
			lk.version = version
			return

		}

	}
	// Your code here
}

func (lk *Lock) Release() {
	var Err rpc.Err = ""
	for {
		Err = lk.ck.Put(lk.ls, "", lk.version)
		if Err != rpc.ErrMaybe { //只要不是ErrMaybe就代表释放完毕(除此之外可能是ErrVersion或者OK)
			return
		}

	}

	// Your code here
}
