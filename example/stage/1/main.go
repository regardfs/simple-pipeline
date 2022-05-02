package main

import (
	"context"
	"errors"
	"fmt"
	. "github.com/regardfs/simple-pipeline"
	"log"
	"time"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second* 2)
	defer cancel()
	var m1 = Multiplier1{Factor: 10, Name: "Multiplier1"}
	task1 := NewTask(m1.Process, m1.Cancel, m1.GetName)
	var m2 = Multiplier2{Factor: 222, Name: "Multiplier2"}
	task2 := NewTask(m2.Process, m2.Cancel, m2.GetName)
	var m3 = Multiplier3{Factor: 333, Name: "Multiplier3"}
	task3 := NewTask(m3.Process, m3.Cancel, m3.GetName)
	var m4 = Multiplier4{Factor: 444, Name: "Multiplier4"}
	task4 := NewTask(m4.Process, m4.Cancel, m4.GetName)
	var m5 = Multiplier5{Factor: 555, Name: "Multiplier5"}
	task5 := NewTask(m5.Process, m5.Cancel, m5.GetName)
	var m6 = Multiplier5{Factor: 666, Name: "Multiplier6"}
	task6 := NewTask(m6.Process, m6.Cancel, m6.GetName)
	tasks1 := make([]*Task, 0)
	tasks2 := make([]*Task, 0)
	tasks3 := make([]*Task, 0)
	tasks4 := make([]*Task, 0)

	taskGroup1 := NewTaskGroup(append(tasks1, &task1, &task4), 1, true)
	taskGroup2 := NewTaskGroup(append(tasks2, &task3), 2, false)
	taskGroup3 := NewTaskGroup(append(tasks3, &task5, &task6), 3, true)
	taskGroup4 := NewTaskGroup(append(tasks4, &task2), 4, false)

	stage1 := NewStage("Stage1", 1, func(i interface{}) error{return nil}, time.Second * 10)
	stage1.Register(taskGroup1).Register(taskGroup2).Register(taskGroup3).Register(taskGroup4)
	stage1.Run(ctx, 1)
	fmt.Println("stage1", stage1)
}


type Multiplier1 struct {
	// Factor will change the amount each number is multiplied by
	Factor int
	Name   Name
}

// Process multiplies a number by factor
func (m *Multiplier1) Process(_ context.Context, in interface{}) (interface{}, error) {
	return in.(int) * m.Factor, nil
}

// Cancel is called when the context is canceled
func (m *Multiplier1) Cancel(i interface{}, err error) {
	log.Printf("error: could not multiply %d, %s\n", i, err)
}

func (m *Multiplier1) GetName() Name {
	return m.Name
}

type Multiplier2 struct {
	// Factor will change the amount each number is multiplied by
	Factor int
	Name   Name
}

// Process multiplies a number by factor
func (m *Multiplier2) Process(_ context.Context, in interface{}) (interface{}, error) {
	return nil, errors.New("Multiplier2 error")
}

// Cancel is called when the context is canceled
func (m *Multiplier2) Cancel(i interface{}, err error) {
	log.Printf("error: could not Multiplier2, %s\n", err)
}

func (m *Multiplier2) GetName() Name {
	return m.Name
}


type Multiplier3 struct {
	// Factor will change the amount each number is multiplied by
	Factor int
	Name   Name
}

// Process multiplies a number by factor
func (m *Multiplier3) Process(_ context.Context, in interface{}) (interface{}, error) {
	return in.(int) * m.Factor, nil
}

// Cancel is called when the context is canceled
func (m *Multiplier3) Cancel(i interface{}, err error) {
	log.Printf("error: could not multiply %d, %s\n", i, err)
}

func (m *Multiplier3) GetName() Name {
	return m.Name
}

type Multiplier4 struct {
	// Factor will change the amount each number is multiplied by
	Factor int
	Name   Name
}

// Process multiplies a number by factor
func (m *Multiplier4) Process(_ context.Context, in interface{}) (interface{}, error) {
	return in.(int) * m.Factor, nil
}

// Cancel is called when the context is canceled
func (m *Multiplier4) Cancel(i interface{}, err error) {
	log.Printf("error: could not multiply %d, %s\n", i, err)
}

func (m *Multiplier4) GetName() Name {
	return m.Name
}

type Multiplier5 struct {
	// Factor will change the amount each number is multiplied by
	Factor int
	Name   Name
}

// Process multiplies a number by factor
func (m *Multiplier5) Process(_ context.Context, in interface{}) (interface{}, error) {
	return in.(int) * m.Factor, nil
}

// Cancel is called when the context is canceled
func (m *Multiplier5) Cancel(i interface{}, err error) {
	log.Printf("error: could not multiply %d, %s\n", i, err)
}

func (m *Multiplier5) GetName() Name {
	return m.Name
}


type Multiplier7 struct {
	// Factor will change the amount each number is multiplied by
	Factor int
	Name   Name
}

// Process multiplies a number by factor
func (m *Multiplier7) Process(_ context.Context, in interface{}) (interface{}, error) {
	return in.(int) * m.Factor, nil
}

// Cancel is called when the context is canceled
func (m *Multiplier7) Cancel(i interface{}, err error) {
	log.Printf("error: could not multiply %d, %s\n", i, err)
}

func (m *Multiplier7) GetName() Name {
	return m.Name
}