package simple_pipeline

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

type Stage struct {
	name         	Name
	taskGroups   	map[int]*TaskGroup
	sequenceId   	int
	onComplete   	func(i interface{}) error
	result       	map[Name]Result
	state        	bool
	timeOut      	time.Duration
	successTasks 	[]Name
	failedTasks 	[]Name
}

type TaskGroup struct {
	tasks []*Task
	sequenceId int
	parallel bool
}

func NewTaskGroup(tasks []*Task, sequenceId int, parallel bool) *TaskGroup {
	return &TaskGroup{
		tasks: tasks,
		sequenceId: sequenceId,
		parallel: parallel,
	}
}

func NewStage(name Name, sequenceId int, onComplete func(i interface{}) error, duration time.Duration) *Stage {
	return &Stage{
		name: name,
		sequenceId: sequenceId,
		onComplete: onComplete,
		result: make(map[Name]Result, 0),
		taskGroups: make(map[int]*TaskGroup, 0),
		state: true,
		timeOut: duration,
	}
}

func process(
	ctx context.Context,
	task Task,
	i interface{},
	out  chan Result,
	wg *sync.WaitGroup,
)  {
	if wg != nil {
		defer wg.Done()
	}
	select {
	case <-ctx.Done():
		task.Cancel(i, ctx.Err())
	default:
		result, err := task.Process(ctx, i)
		out <- Result{result, err}
		if err != nil {
			task.Cancel(i, err)
		}
	}
	return
}

func (stage *Stage) Register(taskGroup *TaskGroup) *Stage {
	stage.taskGroups[taskGroup.sequenceId] = taskGroup
	return stage
}

func (stage *Stage) Run(ctx context.Context, i interface{}) {
	for index := 1; index <= len(stage.taskGroups); index++ {
		if stage.taskGroups[index].parallel {
			stage.runAsync(stage.taskGroups[index].tasks, ctx, i)
		} else {
			stage.runSync(stage.taskGroups[index].tasks, ctx, i)
		}
	}
}

func (stage *Stage) runAsync(tasks []*Task, ctx context.Context, i interface{})  {
	if stage.state == false {
		return
	}
	wg := sync.WaitGroup{}
	wg.Add(len(tasks))
	for _, task := range tasks {
		out := make(chan Result)
		go func(task Task) {
			process(ctx, task, i, out, &wg)
		}(*task)
		select {
		case stage.result[(*task).GetName()] = <-out:
			if stage.result[(*task).GetName()].err != nil {
				fmt.Println(stage.result[(*task).GetName()].err)
				stage.state = false
				stage.failedTasks = append(stage.failedTasks, (*task).GetName())
			} else {
				stage.successTasks = append(stage.successTasks, (*task).GetName())
			}
			close(out)
		case <-time.After(stage.timeOut):
			rst := fmt.Sprintf("%v task executed timeout", (*task).GetName())
			stage.result[(*task).GetName()] = Result{rst, errors.New(rst)}
			stage.state = false
			stage.failedTasks = append(stage.failedTasks, (*task).GetName())
			close(out)
		}
	}
	wg.Wait()
}

func (stage *Stage) runSync(tasks []*Task, ctx context.Context, i interface{}) {
	for _, task := range tasks {
		if stage.state == false {
			return
		}
		out := make(chan Result)
		go func(task Task) {
			select {
			case stage.result[(task).GetName()] = <-out:
				if stage.result[(task).GetName()].err != nil {
					stage.state = false
					stage.failedTasks = append(stage.failedTasks, task.GetName())
				} else {
					stage.successTasks = append(stage.successTasks, task.GetName())
				}
				close(out)
			case <-time.After(stage.timeOut):
				rst := fmt.Sprintf("%v task executed timeout", task.GetName())
				stage.result[(task).GetName()] = Result{rst, errors.New(rst)}
				stage.state = false
				out <- stage.result[(task).GetName()]
				stage.failedTasks = append(stage.failedTasks, task.GetName())
				close(out)
			}
		}(*task)
		process(ctx, *task, i, out,nil)
	}
}

func (stage *Stage) State() bool {
	return stage.state
}