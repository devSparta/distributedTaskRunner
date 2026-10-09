package main

import (
	"fmt"
	"net/http"
	"sync"
	"time"
)

type Task struct {
	ID  int
	URL string
}

type Result struct {
	TaskID   int
	WorkerID int
	URL      string
	Status   int
	Duration time.Duration
	Err      error
}

func worker(id int, jobs <-chan Task, results chan<- Result, wg *sync.WaitGroup, client *http.Client) {
	defer wg.Done()

	for job := range jobs {
		start := time.Now()

		resp, err := client.Get(job.URL)
		if err != nil {
			elapsed := time.Since(start)

			results <- Result{
				TaskID:   job.ID,
				WorkerID: id,
				URL:      job.URL,
				Status:   -1,
				Duration: elapsed,
				Err:      err,
			}

			continue
		}

		resp.Body.Close()

		elapsed := time.Since(start)

		results <- Result{
			TaskID:   job.ID,
			WorkerID: id,
			URL:      job.URL,
			Status:   resp.StatusCode,
			Duration: elapsed,
			Err:      nil,
		}

	}
}

func printResults(results <-chan Result) {
	for result := range results {
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

func main() {
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

	workersAmount := 3

	jobs := make(chan Task)
	results := make(chan Result)

	client := http.Client{
		Timeout: time.Second * 3,
	}

	wg := sync.WaitGroup{}

	wg.Add(workersAmount)
	for w := 1; w <= workersAmount; w++ {
		go worker(w, jobs, results, &wg, &client)
	}

	go func() {
		for _, task := range tasks {
			jobs <- task
		}
		defer close(jobs)
	}()

	go func() {
		wg.Wait()
		close(results)
	}()

	printResults(results)
	fmt.Println("\n\nAll tasks completed!")
}
