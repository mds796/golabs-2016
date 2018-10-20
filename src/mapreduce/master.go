package mapreduce

import (
	"container/list"
	"log"
	"sync"
)
import "fmt"

const (
	IDLE = iota
	IN_PROGRESS
	COMPLETE
)

type WorkerInfo struct {
	address string
	// You can add definitions here.
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	go mr.processRegisteredWorkers()

	mr.runTasks(Map, mr.nMap, mr.nReduce)
	mr.runTasks(Reduce, mr.nReduce, mr.nMap)

	return mr.KillWorkers()
}

func (mr *MapReduce) runTasks(operation JobType, numThisPhase int, numOtherPhase int) {
	tasks := mr.createTasks(numThisPhase, operation, numOtherPhase)

	log.Printf("Running %d tasks for %v operation.", numThisPhase, operation)

	for tasksCompleted := 0; tasksCompleted < numThisPhase; {
		select {
		case task := <-tasks:
			if task.state == COMPLETE {
				mr.unassignedWorkers.Push(task.worker)
				task.worker = nil
				tasksCompleted++
			} else if task.state == IN_PROGRESS {
				delete(mr.Workers, task.worker.address)

				task.worker = nil
				task.state = IDLE

				tasks <- task
			} else {
				go task.start(mr.unassignedWorkers, tasks)
			}
		}
	}
}

func (mr *MapReduce) createTasks(numThisPhase int, operation JobType, numOtherPhase int) chan *Task {
	tasks := make(chan *Task, numThisPhase)
	for i := 0; i < numThisPhase; i++ {
		args := &DoJobArgs{File: mr.file, JobNumber: i, Operation: operation, NumOtherPhase: numOtherPhase}
		tasks <- &Task{state: IDLE, args: args, reply: new(DoJobReply)}
	}
	return tasks
}

func (mr *MapReduce) processRegisteredWorkers() {
	for {
		workerAddress := <-mr.registerChannel
		mr.Workers[workerAddress] = &WorkerInfo{address: workerAddress}
		mr.unassignedWorkers.Push(mr.Workers[workerAddress])

		log.Printf("Registered worker %v.\n", workerAddress)
	}
}

type Task struct {
	worker *WorkerInfo
	args   *DoJobArgs
	reply  *DoJobReply
	state  int
}

func (t *Task) start(workers *WorkerQueue, tasks chan *Task) {
	t.worker = workers.Pop()
	t.state = IN_PROGRESS

	log.Printf("Running task %d for job %v on worker %v.\n", t.args.JobNumber, t.args.Operation, t.worker.address)

	ok := call(t.worker.address, "Worker.DoJob", t.args, t.reply)

	if !ok || !t.reply.OK {
		log.Printf("Failed to do job %v for task %d on worker %v.\n", t.args.Operation, t.args.JobNumber, t.worker.address)
	} else {
		t.state = COMPLETE
	}

	tasks <- t
}

type WorkerQueue struct {
	queue    []*WorkerInfo
	mutex    *sync.Mutex
	notEmpty *sync.Cond
}

func (q *WorkerQueue) Push(worker *WorkerInfo) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	q.queue = append(q.queue, worker)
	q.notEmpty.Signal()
}

func (q *WorkerQueue) Pop() *WorkerInfo {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	for len(q.queue) == 0 {
		q.notEmpty.Wait()
	}

	n := len(q.queue)
	item := q.queue[n-1]

	q.queue = q.queue[0 : n-1]

	return item
}

func (q *WorkerQueue) Len() int {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	return len(q.queue)
}
