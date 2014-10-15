package spin

import (
	"runtime"
	"sync/atomic"
)

type Lock uint32

func (slock *Lock) Lock() {
	for !atomic.CompareAndSwapUint32((*uint32)(slock), 0, 1) {
		runtime.Gosched()
	}
}

func (slock *Lock) Unlock() {
	if old := atomic.SwapUint32((*uint32)(slock), 0); old != 1 {
		panic("spin lock corrupted")
	}
}
