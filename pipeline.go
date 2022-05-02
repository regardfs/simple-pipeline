package simple_pipeline

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type Result struct {
	rst interface{}
	err error
}

type Pipeline struct {
	stages map[int]*Stage
	result 		map[Name]Result
	timeOut		time.Duration
	state		bool
	successStages	[]Name
	failedStages	[]Name
}


func NewPipeline(d time.Duration) *Pipeline {
	return &Pipeline{
		stages: make(map[int]*Stage, 0),
		result: make(map[Name]Result, 0),
		timeOut: d,
		state: true,
	}
}

func (pipeline *Pipeline) Register(stage *Stage) *Pipeline {
	pipeline.stages[stage.sequenceId] = stage
	return pipeline
}

func (pipeline *Pipeline) Run(ctx context.Context, i interface{}) func(){
	for index := 1; index <= len(pipeline.stages); index++ {
		pipeline.runSync(pipeline.stages[index], ctx, i)
	}
	//Todo: collect all stage result
	return nil
}

func (pipeline *Pipeline) runSync(stage *Stage, ctx context.Context, i interface{}) {
	if pipeline.state == false {return}
	stage.Run(ctx, i)
	if stage.state == false {
		rst := fmt.Sprintf("%v stage executed failed, success tasks: %v, failed tasks: %v", stage.name, stage.successTasks, stage.failedTasks)
		pipeline.result[stage.name] = Result{rst: rst, err: errors.New(rst)}
		pipeline.state = false
		return
	}
	rst := fmt.Sprintf("%v stage executed success", stage.name)
	pipeline.result[stage.name] = Result{rst: rst, err: errors.New(rst)}
}