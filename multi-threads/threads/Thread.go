package threads

import (
	"fmt"
	"os"
	"sync/atomic"
	"time"
)

type Thread struct {
	Name          string
	Implement     Task
	BroadcastChan chan string
	ExitChan      chan bool
	running       atomic.Bool
}

func (t *Thread) Run() {
	t.running.Store(true)

	if t.Implement != nil {
		t.Implement.EnterLoop()
	}

	go func() {
		defer close(t.ExitChan)
		defer t.running.Store(false)
		fmt.Fprintf(os.Stderr, "thread-%s is running now\n", t.Name)

		for {
			time.Sleep(1 * time.Second)

			select {
			case message := <-t.BroadcastChan:
				fmt.Fprintf(os.Stderr, "thread-%s receives the broadcast message \"%s\".\n", t.Name, message)
			case <-t.ExitChan:
				if t.Implement != nil {
					t.Implement.EndTask()
					t.Implement = nil
				}
				return
			default:
				continue
			}
		}
	}()
}

// Terminate stops the thread and waits for it to exit. It is safe to call
// more than once.
func (t *Thread) Terminate() {
	fmt.Fprintf(os.Stderr, "thread-%s shutting down\n", t.Name)
	if t.Implement != nil {
		t.Implement.EndTask()
		t.Implement = nil
	}
	t.ExitChan <- true
}

// BroadcastMessage delivers message to the thread, or drops it if the thread
// has already exited.
func (t *Thread) BroadcastMessage(message string) {
	t.BroadcastChan <- message
}

func (t *Thread) IsAlive() bool {
	return t.running.Load()
}
