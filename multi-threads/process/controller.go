package process

import (
	"fmt"
	"os"
	"sync"
	"time"
)

type Controller struct {
	endMutex sync.Mutex
	end      bool
}

func (c *Controller) EnterLoop() {
	c.end = false

	go func() {
		for {
			fmt.Fprintf(os.Stderr, "*** controller is running ***\n")

			<-time.After(2 * time.Second)

			c.endMutex.Lock()
			if c.end {
				c.endMutex.Unlock()
				return
			}
			c.endMutex.Unlock()
		}
	}()
}

func (c *Controller) EndTask() {
	if c.end {
		return
	}

	defer c.endMutex.Unlock()

	c.endMutex.Lock()
	c.end = true
}
