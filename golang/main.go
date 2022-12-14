package main

import (
	"fmt"
	"sync"
	"time"
)

type (
	Task struct {
		ID         int64
		CreatedAt  string
		FinishedAt string
		Result     []byte
	}

	job func(in, out chan any)
)

func ExecutePipeline(jobs ...job) {
	in := make(chan any)
	out := make(chan any)
	wg := new(sync.WaitGroup)

	for _, j := range jobs {
		wg.Add(1)
		go func(in, out chan any, j job, wg *sync.WaitGroup) {
			defer wg.Done()
			j(in, out)
			close(out)
		}(in, out, j, wg)

		in = out
		out = make(chan any)
	}

	close(out)
	wg.Wait()
}

func main() {
	results := make(map[int64]Task)
	errors := make([]error, 0)
	ExecutePipeline(
		TaskCreator,
		TaskWorker,
		TaskSorter,
		TaskReceiver(results, &errors),
	)

	fmt.Println("\nErrors:")
	for _, err := range errors {
		fmt.Println(err)
	}

	fmt.Println("\nResults")
	for id := range results {
		fmt.Println(id)
	}
}

func TaskCreator(in, out chan any) {
	wg := new(sync.WaitGroup)
	t := time.NewTicker(100 * time.Millisecond)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-t.C: // для остановки потока задач
				return
			default:
				format := time.Now().Format(time.RFC3339)
				id := time.Now().UnixNano()
				if id%2 > 0 {
					format = "Some error occured"
				}
				out <- Task{ID: id, CreatedAt: format}
			}
		}
	}()

	wg.Wait()
}

func TaskWorker(in, out chan any) {
	wg := new(sync.WaitGroup)
	for entry := range in {
		wg.Add(1)
		go func(entry interface{}) {
			defer wg.Done()
			task := entry.(Task)
			tt, err := time.Parse(time.RFC3339, task.CreatedAt)
			if err != nil || !tt.After(time.Now().Add(-20*time.Second)) {
				task.Result = []byte("something went wrong")
			} else {
				task.Result = []byte("task has been successed")
			}
			task.FinishedAt = time.Now().Format(time.RFC3339Nano)

			time.Sleep(time.Millisecond * 150)

			out <- task
		}(entry)
	}

	wg.Wait()
}

func TaskSorter(in, out chan any) {
	wg := new(sync.WaitGroup)

	for entry := range in {
		wg.Add(1)
		go func(entry interface{}) {
			defer wg.Done()
			task := entry.(Task)
			if string(task.Result[14:]) == "successed" {
				out <- task
			} else {
				out <- fmt.Errorf("Task id %d time %s, error %s", task.ID, task.CreatedAt, task.Result)
			}
		}(entry)
	}

	wg.Wait()
}

func TaskReceiver(tasks map[int64]Task, errors *[]error) job {
	muSlice := new(sync.Mutex)
	muMap := new(sync.Mutex)
	wg := new(sync.WaitGroup)
	return func(in, out chan any) {
		for entry := range in {
			wg.Add(1)
			go func(entry interface{}) {
				defer wg.Done()
				switch v := entry.(type) {
				case error:
					muSlice.Lock()
					*errors = append(*errors, v)
					muSlice.Unlock()
				case Task:
					muMap.Lock()
					tasks[v.ID] = v
					muMap.Unlock()
				}
			}(entry)
		}

		wg.Wait()
	}
}
