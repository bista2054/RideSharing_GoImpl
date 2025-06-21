package main

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"
)

// RideTask represents a ride-sharing task
type RideTask struct {
	RideID         string
	PickupLocation string
	Destination    string
	PassengerCount int
	RequestTime    time.Time
}

func (t RideTask) String() string {
	return fmt.Sprintf("Ride[%s] from %s to %s (%d passengers) requested at %v",
		t.RideID, t.PickupLocation, t.Destination, t.PassengerCount, t.RequestTime)
}

// TaskQueue manages the queue of ride tasks
type TaskQueue struct {
	tasks chan RideTask
	quit  chan struct{}
}

func NewTaskQueue(size int) *TaskQueue {
	return &TaskQueue{
		tasks: make(chan RideTask, size),
		quit:  make(chan struct{}),
	}
}

func (q *TaskQueue) AddTask(task RideTask) error {
	select {
	case q.tasks <- task:
		log.Printf("Task added to queue: %v", task)
		return nil
	case <-q.quit:
		return errors.New("queue stopped")
	}
}

func (q *TaskQueue) GetTask() (RideTask, error) {
	select {
	case task := <-q.tasks:
		return task, nil
	case <-q.quit:
		return RideTask{}, errors.New("queue stopped")
	}
}

func (q *TaskQueue) Stop() {
	log.Println("Stopping task queue...")
	close(q.quit)
}

func worker(id int, queue *TaskQueue, results chan<- string, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Printf("Worker %d started", id)

	for {
		task, err := queue.GetTask()
		if err != nil {
			log.Printf("Worker %d stopping: %v", id, err)
			break
		}

		// Log ride-related info
		log.Printf("Worker %d processing ride: %s from %s to %s (%d passengers) at %v",
			id, task.RideID, task.PickupLocation, task.Destination, task.PassengerCount, task.RequestTime)

		result, err := processTask(id, task)
		if err != nil {
			log.Printf("Worker %d task error: %v", id, err)
			continue
		}

		results <- result
	}

	log.Printf("Worker %d finished", id)
}

func processTask(workerID int, task RideTask) (string, error) {
	time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)
	return fmt.Sprintf("Worker %d processed: %v", workerID, task), nil
}

func main() {
	const workerCount = 5
	const taskCount = 20

	queue := NewTaskQueue(taskCount)
	results := make(chan string, taskCount)
	var wg sync.WaitGroup

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go worker(i, queue, results, &wg)
	}

	go func() {
		file, err := os.Create("ride_results.txt")
		if err != nil {
			log.Fatal("Error creating results file:", err)
		}
		defer file.Close()

		for result := range results {
			if _, err := file.WriteString(result + "\n"); err != nil {
				log.Println("Error writing result:", err)
			}
		}
	}()

	for i := 0; i < taskCount; i++ {
		task := RideTask{
			RideID:         fmt.Sprintf("Ride-%d", i),
			PickupLocation: fmt.Sprintf("Location-%d", i),
			Destination:    fmt.Sprintf("Destination-%d", i),
			PassengerCount: 1 + i%4,
			RequestTime:    time.Now(),
		}
		if err := queue.AddTask(task); err != nil {
			log.Println("Error adding task:", err)
		}
	}

	time.Sleep(2 * time.Second)
	queue.Stop()
	wg.Wait()
	close(results)

	log.Println("Ride sharing system shutdown complete")
}
