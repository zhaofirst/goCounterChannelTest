package counter

import (
	"errors"
	"log"
	"sync"
	"time"
)

// FuncCbFlush is the func type for callback flush.
type FuncCbFlush func() error

var (
	// ErrNotFound defines common not found error.
	ErrNotFound = errors.New("not found")
)

// Counter is the counter struct.
type Counter struct {
	once  sync.Once
	mu    sync.RWMutex
	store map[string]int64
}

// NewCounter returns a counter instance.
func NewCounter() *Counter {
	return &Counter{}
}

// Init initializes counter.
func (c *Counter) Init() error {
	// init map
	c.store = make(map[string]int64)
	return nil
}

// Get gets counter's value by key.
func (c *Counter) Get(key string) (int64, error) {
	// r = sync.RWMutex // BUNENG ZHE YANG
	// read lock
	c.mu.RLock()
	defer c.mu.RUnlock()

	val, found := c.store[key]
	if !found {
		// key not found, return err.
		return val, ErrNotFound
	}

	return val, nil
}

// Incr implements increment method for counter.
func (c *Counter) Incr(key string, value int64) {
	// lock for avoid data race.
	c.mu.Lock()
	defer c.mu.Unlock()

	// check key whether exist
	cur, found := c.store[key]
	if !found {
		cur = 0
	}

	c.store[key] = cur + value
}

// Reset implements reset method for counter.
func (c *Counter) Reset() {
	// reset store, need lock
	c.mu.Lock()
	// reset
	c.store = make(map[string]int64)
	c.mu.Unlock()
}

// Flush2Broker flush data from callback and reset counter.
func (c *Counter) Flush2Broker(interval int, cb FuncCbFlush) {
	flushFn := func() {
		for {
			// unit is millsecond, eg, 5000 == 5s
			time.Sleep(time.Duration(interval) * time.Millisecond)
			// before flush should lock
			c.mu.Lock()
			err := cb()
			c.mu.Unlock()
			if err != nil {
				log.Printf("flush data to broker error: %v\n", err)
			} else {
				// reset store if flush success
				c.Reset()
			}
		}
	}

	// only need run one times
	c.once.Do(func() { go flushFn() })
}
