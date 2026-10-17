package task

import "errors"

var (
	ErrTaskNotFound      = errors.New("task not found")
	ErrTaskAlreadyExists = errors.New("task already exists")
	ErrInvalidState      = errors.New("invalid task state")
	ErrWrongWorker       = errors.New("wrong assigned worker")
	ErrStaleAttempt      = errors.New("stale task attempt")
	ErrResultConflict    = errors.New("conflicting task result")
)
