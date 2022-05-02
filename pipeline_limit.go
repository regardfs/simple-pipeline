package simple_pipeline

import (
	"context"
	"errors"
	"fmt"
	"golang.org/x/sync/semaphore"
	"sync"
)

var _ PipelineLimiter = (*pipelineLimiter)(nil)

type PipelineLimiter interface {
	// Size return the size of pipelines
	Size() int
	// Processed Gets the total number of tasks that have been processed
	Processed() int
	//Wait until task ends or ctx is canceled
	Wait(ctx context.Context) error
	// Go  create goroutine
	Go(ctx context.Context, goroutine func()) error
}

type pipelineLimiter struct {
	processed int
	size      int
	locker    sync.Mutex
	waitGroup sync.WaitGroup
	semaphore *semaphore.Weighted
}

func (l *pipelineLimiter) Size() int {
	return l.size
}

func (l *pipelineLimiter) Processed() int {
	l.locker.Lock()
	defer l.locker.Unlock()
	return l.processed
}

func (l *pipelineLimiter) addProcessed(i int) {
	l.locker.Lock()
	defer l.locker.Unlock()
	l.processed += i
}

func (l *pipelineLimiter) Wait(ctx context.Context) error {
	stopChan := make(chan struct{})
	// 等待，等待完成后关闭 stopChan
	go func() {
		l.waitGroup.Wait()
		close(stopChan)
	}()
	// 如果等待完成/或者上下文被关闭则退出
	select {
	case <-stopChan:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (l *pipelineLimiter) Go(ctx context.Context, goroutine func()) error {
	// 尝试获取执行机会
	if err := l.semaphore.Acquire(ctx, 1); err != nil {
		fmt.Printf("err: %v", err)
		return err
	}
	// 抢到 semaphore 后才开始执行
	l.waitGroup.Add(1)
	// 启动 goroutine
	go func() {
		defer func() {
			l.addProcessed(1)
			l.semaphore.Release(1)
			l.waitGroup.Done()
		}()
		goroutine()
	}()
	return nil
}

func NewPipelineLimiter(size int) PipelineLimiter {
	if size <= 0 {
		panic(errors.New("the size of workerLimiter must be greater than 0"))
	}

	return &pipelineLimiter{
		size:      size,
		processed: 0,
		semaphore: semaphore.NewWeighted(int64(size)),
	}
}
