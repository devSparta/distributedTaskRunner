package worker

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	pb "github.com/devSparta/distributedTaskRunner/proto"
)

type Runner struct {
	client      pb.TaskServiceClient
	httpClient  *http.Client
	workerID    string
	concurrency int
}

func NewRunner(client pb.TaskServiceClient, workerID string, concurrency int) (*Runner, error) {
	if concurrency <= 0 {
		return nil, fmt.Errorf("concurrency must be > 0")
	}

	if workerID == "" {
		return nil, fmt.Errorf("workerID must not be empty")
	}

	return &Runner{
		client: client,
		httpClient: &http.Client{
			Timeout: time.Second * 5,
		},
		workerID:    workerID,
		concurrency: concurrency,
	}, nil
}

func (r *Runner) Run(ctx context.Context) error {
	wg := sync.WaitGroup{}

	wg.Add(r.concurrency)
	for i := 0; i < r.concurrency; i++ {
		go func() {
			defer wg.Done()
			r.runWorker(ctx)
		}()
	}

	wg.Wait()

	return nil
}

func (r *Runner) runWorker(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}

		task, err := r.client.AcquireTask(
			ctx,
			&pb.AcquireTaskRequest{WorkerId: r.workerID},
		)

		if err != nil {
			log.Printf("acquire task error: %v", err)

			select {
			case <-ctx.Done():
				return
			case <-time.After(400 * time.Millisecond):
				continue
			}
		}

		start := time.Now()
		resp, err := r.httpClient.Get(task.Url)

		result := &pb.TaskResult{}

		if err != nil {
			result.ErrorMessage = err.Error()
		} else {
			result.HttpStatus = int32(resp.StatusCode)
			resp.Body.Close()
		}

		result.DurationMs = time.Since(start).Milliseconds()

		req := &pb.ReportTaskResultRequest{
			TaskId:   task.TaskId,
			WorkerId: r.workerID,
			Result:   result,
		}

		attempts := 3
		for attempts > 0 {
			reportCtx, cancelReport := context.WithTimeout(context.Background(), time.Second*5)

			_, errIn := r.client.ReportTaskResult(reportCtx, req)
			if errIn != nil {
				log.Println(errIn)
				cancelReport()
				attempts--

				if attempts == 0 {
					log.Printf("failed to report task result: %v", errIn)
					return
				}

				time.Sleep(time.Millisecond * 100)

				continue
			}

			attempts = 0
			cancelReport()
		}

		continue
	}
}
