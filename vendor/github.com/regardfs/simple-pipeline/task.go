package simple_pipeline

import (
	"context"
)
type Name string

type Task interface {
	Process(ctx context.Context, i interface{}) (interface{}, error)
	Cancel(i interface{}, err error)
	GetName() Name
}

type task struct {
	process func(ctx context.Context, i interface{}) (interface{}, error)
	cancel  func(i interface{}, err error)
	getName func() Name
}

func (t *task) Process(ctx context.Context, i interface{}) (interface{}, error) {
	return t.process(ctx, i)
}

func (t *task) Cancel(i interface{}, err error) {
	t.cancel(i, err)
}

func (t *task) GetName() Name {
	return t.getName()
}

func NewTask(
	process func(ctx context.Context, i interface{}) (interface{}, error),
	cancel func(i interface{}, err error),
	getName func() Name,
) Task {
	return &task{process, cancel, getName}
}


