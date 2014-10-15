package spin

import (
	. "launchpad.net/gocheck"
	"sync"
	"testing"
	"time"
)

// Hook up gocheck into the gotest runner.
func Test(t *testing.T) {
	TestingT(t)
}

type MySuite struct{}

var _ = Suite(&MySuite{})

func (s *MySuite) TestSpinLock(c *C) {
	var slock Lock
	var val int = 1
	var val_1_1, val_1_2, val_2_1, val_2_2 int
	timing := make(chan int)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		slock.Lock()
		val_1_1 = val
		val++
		timing <- 1 // 1
		time.Sleep(1 * time.Millisecond)
		slock.Unlock()
		<-timing // 2
		slock.Lock()
		val_1_2 = val
		val++
		timing <- 1 // 3
		time.Sleep(1 * time.Millisecond)
		slock.Unlock()
		<-timing // 4
		wg.Done()
	}()

	go func() {
		<-timing // 1
		slock.Lock()
		val_2_1 = val
		val++
		timing <- 1 // 2
		time.Sleep(1 * time.Millisecond)
		slock.Unlock()
		<-timing // 3
		slock.Lock()
		val_2_2 = val
		val++
		timing <- 1 // 4
		time.Sleep(1 * time.Millisecond)
		slock.Unlock()
		wg.Done()
	}()

	wg.Wait()
	c.Check(val_1_1, Equals, 1)
	c.Check(val_2_1, Equals, 2)
	c.Check(val_1_2, Equals, 3)
	c.Check(val_2_2, Equals, 4)
}
