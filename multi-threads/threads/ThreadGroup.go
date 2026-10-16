package threads

import (
	"fmt"
	"os"
	"slices"
	"sync"
)

type ThreadGroup struct {
	Name           string
	Threads        []*Thread
	ThreadCapacity int
	threadMutex    sync.Mutex
}

func (tg *ThreadGroup) AddThread(name string) {
	tg.AddThreadWithImplement(name, nil)
}

func (tg *ThreadGroup) AddThreadWithImplement(name string, implement Task) {
	defer tg.threadMutex.Unlock()

	if name == "" {
		return
	}

	tg.threadMutex.Lock()

	if len(tg.Threads) > 0 {
		for _, thread := range tg.Threads {
			if thread == nil {
				continue
			}

			if name == (*thread).Name {
				fmt.Fprintf(os.Stderr, "thread name (%s) already exists, abort thread creation\n", name)
				return
			}
		}
	}

	thread := &Thread{}

	if implement == nil {
		thread = &Thread{
			Name:          name,
			BroadcastChan: make(chan string),
			ExitChan:      make(chan bool),
		}
	} else {
		thread = &Thread{
			Name:          name,
			Implement:     implement,
			BroadcastChan: make(chan string),
			ExitChan:      make(chan bool),
		}
	}

	tg.Threads = append(tg.Threads, thread)
	(*thread).Run()
}

// snapshot returns a copy of the thread list so callers can iterate
// without holding the lock while blocking on thread channels.
func (tg *ThreadGroup) snapshot() []*Thread {
	tg.threadMutex.Lock()
	defer tg.threadMutex.Unlock()

	return append([]*Thread(nil), tg.Threads...)
}

func (tg *ThreadGroup) DeleteThread(name string) {
	tg.threadMutex.Lock()
	var target *Thread
	for i, thread := range tg.Threads {
		if thread != nil && name == (*thread).Name {
			target = thread
			tg.Threads = slices.Delete(tg.Threads, i, i+1)
			break
		}
	}
	tg.threadMutex.Unlock()

	if target == nil {
		return
	}

	fmt.Fprintf(os.Stderr, "ThreadGroup has received to delete the thread name (%s), and deleting it.\n", name)
	(*target).Terminate()
}

func (tg *ThreadGroup) BroadcastMessage(message string) {
	threads := tg.snapshot()
	if len(threads) == 0 {
		return
	}

	fmt.Fprintf(os.Stderr, "ThreadGroup has received the broadcast message for %d thread(s):\n%s\n", len(threads), message)

	for _, thread := range threads {
		if thread == nil {
			continue
		}

		(*thread).BroadcastMessage(message)
	}
}

func (tg *ThreadGroup) IsAllAliveCheck() bool {
	defer tg.threadMutex.Unlock()

	tg.threadMutex.Lock()
	if len(tg.Threads) == 0 {
		return false
	}

	aliveCount := 0
	for _, thread := range tg.Threads {
		if thread == nil {
			continue
		}

		if (*thread).IsAlive() {
			aliveCount++
		}
	}

	if len(tg.Threads) == aliveCount {
		return true
	}

	return false
}

func (tg *ThreadGroup) Shutdown() {
	tg.threadMutex.Lock()
	threads := tg.Threads
	tg.Threads = make([]*Thread, 0, tg.ThreadCapacity)
	tg.threadMutex.Unlock()

	if len(threads) == 0 {
		return
	}

	fmt.Fprintf(os.Stderr, "ThreadGroup has received the shutdown single and will shut down %d thread(s).\n", len(threads))

	for _, thread := range threads {
		if thread == nil {
			continue
		}

		(*thread).Terminate()
	}
}
