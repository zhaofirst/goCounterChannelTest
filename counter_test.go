package counter

import (
	"log"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCounter(t *testing.T) {
	// construct counter
	ct := NewCounter()
	// init counter
	err := ct.Init()
	if err != nil {
		t.Fatalf("init counter error: %v", err)
	}

	// test increment with multi parallel clients
	var wg sync.WaitGroup
	// 10 clients
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// increase value
			ct.Incr("get.call", 1)
		}()
	}
	wg.Wait()
	// get.call should equal 10
	v, err := ct.Get("get.call")
	if err != nil {
		t.Fatalf("get counter value error: %v", err)
	}

	if v != 10 {
		t.Fatalf("counter value should equal: %d, but got: %d", 10, v)
	}

	// test flush
	var flushTimes int64
	// callback for flush
	cb := func() error {
		log.Println("flush data...")
		atomic.AddInt64(&flushTimes, 1)
		return nil
	}

	// flush every 1s
	ct.Flush2Broker(1000, cb)
	// wait for 6s
	time.Sleep(6 * time.Second)
	if flushTimes < 5 {
		t.Fatalf("unexpected flush times: %d", flushTimes)
	}

	t.Logf("flush times: %d", flushTimes)

	// counter should be reset
	_, err = ct.Get("get.call")
	if err != ErrNotFound {
		t.Fatalf("counter should be reset, unexpected error: %v", err)
	}
}
