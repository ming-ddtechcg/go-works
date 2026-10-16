package threads

type Task interface {
	EnterLoop()
	EndTask()
}
