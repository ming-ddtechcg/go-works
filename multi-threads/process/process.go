package process

import (
	"fmt"
	"os"
	"sync"
	"time"
)

type Process struct {
	Name     string
	endMutex sync.Mutex
	end      bool
}

func (p *Process) EnterLoop() {
	p.end = false

	go func() {
		for {
			fmt.Fprintf(os.Stderr, "\t*** process (%s) is running ***\n", p.Name)

			<-time.After(1 * time.Second)

			p.endMutex.Lock()
			if p.end {
				p.endMutex.Unlock()
				fmt.Fprintf(os.Stderr, "\t\t--> process (%s) exits -->\n", p.Name)
				return
			}
			p.endMutex.Unlock()
		}
	}()
}

func (p *Process) EndTask() {
	if p.end {
		return
	}

	defer p.endMutex.Unlock()

	p.endMutex.Lock()
	p.end = true
}
