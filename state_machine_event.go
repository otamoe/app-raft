package appraft

import (
	"errors"
	"fmt"

	pb "github.com/otamoe/app-raft-pb"
	"go.uber.org/zap"
)

type (
	StateMachineEvent struct {
		clusterID uint64
		nodeID    uint64
		values    chan *StateMachineEventValue
		closing   chan struct{}
		closed    chan struct{}
		event     StateMachineEventFunc
		logger    *zap.Logger
	}

	StateMachineEventValue struct {
		NewItem *pb.Item
		OldItem *pb.Item
	}

	StateMachineEventFunc func(clusterID uint64, nodeID uint64, newItem *pb.Item, oldItem *pb.Item)

	StateMachineEventBatch struct {
		stateMachineEvent *StateMachineEvent
		values            []*StateMachineEventValue
	}
)

func NewStateMachineEvent(clusterID uint64, nodeID uint64, event StateMachineEventFunc, logger *zap.Logger) *StateMachineEvent {
	return &StateMachineEvent{
		clusterID: clusterID,
		nodeID:    nodeID,
		event:     event,
		values:    make(chan *StateMachineEventValue, 200),
		closing:   make(chan struct{}),
		closed:    make(chan struct{}),
	}
}

// 开始运行
func (stateMachineEvent *StateMachineEvent) Start() {
	defer close(stateMachineEvent.closed)
	defer close(stateMachineEvent.values)
	for {
		select {
		case value := <-stateMachineEvent.values:
			// 执行
			stateMachineEvent.runOne(value)
			// 频道
			return
		case <-stateMachineEvent.closing:
			// 关闭
			return
		}
	}
}

// 执行某一个  带恢复的
func (stateMachineEvent *StateMachineEvent) runOne(value *StateMachineEventValue) {
	defer func() {
		if r := recover(); r != nil {
			var err error
			switch val := r.(type) {
			case string:
				err = errors.New(val)
			case error:
				err = val
			default:
				err = errors.New(fmt.Sprintf("%+v", err))
			}
			stateMachineEvent.logger.Error("recover", zap.Error(err), zap.Stack("stack"))
		}
	}()
	stateMachineEvent.event(stateMachineEvent.clusterID, stateMachineEvent.nodeID, value.NewItem, value.OldItem)
}
func (stateMachineEvent *StateMachineEvent) Close() (err error) {
	close(stateMachineEvent.closing)
	<-stateMachineEvent.closed
	return
}

func (stateMachineEvent *StateMachineEvent) Batch() *StateMachineEventBatch {
	return &StateMachineEventBatch{
		stateMachineEvent: stateMachineEvent,
	}
}

func (stateMachineEventBatch *StateMachineEventBatch) Add(newItem *pb.Item, oldItem *pb.Item) {
	// 添加
	if stateMachineEventBatch.stateMachineEvent.event == nil {
		return
	}
	stateMachineEventBatch.values = append(stateMachineEventBatch.values, &StateMachineEventValue{
		NewItem: newItem,
		OldItem: oldItem,
	})
	return
}

func (stateMachineEventBatch *StateMachineEventBatch) Commit() {
	for _, value := range stateMachineEventBatch.values {
		stateMachineEventBatch.stateMachineEvent.values <- value
	}
}
