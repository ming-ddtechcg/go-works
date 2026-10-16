package main

import (
	"fmt"
	"multi-threads/process"
	"multi-threads/threads"
	"strconv"
	"time"
)

var numberOfThread int = 10

func main() {
	closed := make(chan bool)

	var duration int
	fmt.Print("Input a number for the process duration: ")
	fmt.Scan(&duration)

	go func() {
		<-time.After(time.Duration(duration) * time.Second)
		closed <- true
	}()

	threadGroup := threads.ThreadGroup{
		Name:           "my-threadgroup",
		Threads:        make([]*threads.Thread, 0, numberOfThread),
		ThreadCapacity: numberOfThread,
	}

	c := &process.Controller{}

	threadGroup.AddThreadWithImplement("controller", c)
	threadGroup.AddThreadWithImplement("controller", c)

	var p *process.Process
	for i := 1; i < 4; i++ {
		p = &process.Process{Name: "thread-" + strconv.Itoa(i)}
		threadGroup.AddThreadWithImplement(strconv.Itoa(i), p)
	}

	time.Sleep(2 * time.Second)

	isAllAlive := threadGroup.IsAllAliveCheck()
	fmt.Printf("All threads are alive state %t\n", isAllAlive)

	time.Sleep(2 * time.Second)

	threadGroup.BroadcastMessage("test")

	time.Sleep(2 * time.Second)

	threadGroup.DeleteThread("2")

	time.Sleep(2 * time.Second)

	<-closed
	threadGroup.Shutdown()
}
