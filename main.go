package main

import (
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

type Task struct {
	ID  int64
	URL string
}

type Result struct {
	TaskID   int64
	WorkerID int
	URL      string
	Status   int
	Duration time.Duration
	Err      error
}

func worker(id int, jobs <-chan Task, results chan<- Result, wg *sync.WaitGroup, client *http.Client, m *TaskManager) {
	defer wg.Done()

	for job := range jobs {
		start := time.Now()

		result := Result{
			TaskID:   job.ID,
			WorkerID: id,
			URL:      job.URL,
			//Status:   resp.StatusCode,
			//Duration: elapsed,
			//Err:      nil/err,
		}

		err := m.Start(job.ID)
		if err != nil {
			result.Err = err
			result.Duration = time.Since(start)

			log.Printf("worker %d: cannot start task %d: %v", id, job.ID, err)

			continue
		}

		resp, err := client.Get(job.URL)
		if err != nil {
			result.Err = err
		} else {
			result.Status = resp.StatusCode

			resp.Body.Close()
		}

		result.Duration = time.Since(start)

		results <- result
	}
}

func processResults(results <-chan Result, m *TaskManager) {
	for result := range results {
		if err := m.Finish(result); err != nil {
			log.Fatal(err)
		}

		state, ok := m.Get(result.TaskID)
		if !ok {
			log.Printf("task does not exist")
		}

		fmt.Printf("Task state: %s\n", state.Status)

		fmt.Printf(
			"TaskID: %d, WorkerID: %d, URL: %s, Status: %d, Duration: %s\n",
			result.TaskID,
			result.WorkerID,
			result.URL,
			result.Status,
			result.Duration,
		)

		if result.Err != nil {
			fmt.Printf("Error: %v\n", result.Err)
		}
	}
}

func runLocalDemo() {
	tasks := []Task{
		{ID: 1, URL: "https://yandex.ru"},
		{ID: 2, URL: "https://mail.ru"},
		{ID: 3, URL: "https://vk.ru"},
		{ID: 4, URL: "https://example.com"},
		{ID: 5, URL: "https://google.com"},
		{ID: 6, URL: "https://bing.com"},
		{ID: 7, URL: "https://rutube.ru"},
		{ID: 8, URL: "https://gmail.com"},
		{ID: 9, URL: "https://habr.ru"},
		{ID: 10, URL: "https://youtube.com"},
	}

	manager := NewTaskManager()

	for _, task := range tasks {
		if err := manager.Add(task); err != nil {
			log.Fatal(err)
		}
	}

	workersAmount := 3

	jobs := make(chan Task)
	results := make(chan Result)

	client := http.Client{
		Timeout: time.Second * 3,
	}

	wg := sync.WaitGroup{}

	wg.Add(workersAmount)
	for w := 1; w <= workersAmount; w++ {
		go worker(w, jobs, results, &wg, &client, manager)
	}

	go func() {
		for _, task := range tasks {
			jobs <- task
		}
		close(jobs)
	}()

	go func() {
		wg.Wait()
		close(results)
	}()

	processResults(results, manager)
	fmt.Println("\n\nAll tasks completed!")
}
