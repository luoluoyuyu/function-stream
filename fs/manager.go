/*
 * Copyright 2024 Function Stream Org.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package fs

import (
	"context"
	"github.com/functionstream/functionstream/common"
	"github.com/functionstream/functionstream/common/model"
	"github.com/functionstream/functionstream/fs/contube"
	"log/slog"
	"math/rand"
	"strconv"
	"sync"
)

type FunctionManager struct {
	functions     map[string][]*FunctionInstance
	functionsLock sync.Mutex
	tubeFactory   contube.TubeFactory
}

func NewFunctionManager(config *Config) (*FunctionManager, error) {
	tubeFactory, err := config.TubeBuilder(context.Background(), config)
	if err != nil {
		return nil, err
	}
	return &FunctionManager{
		functions:   make(map[string][]*FunctionInstance),
		tubeFactory: tubeFactory,
	}, nil
}

func (fm *FunctionManager) StartFunction(f *model.Function) error {
	fm.functionsLock.Lock()
	defer fm.functionsLock.Unlock() // TODO: narrow the lock scope
	if _, exist := fm.functions[f.Name]; exist {
		return common.ErrorFunctionExists
	}
	fm.functions[f.Name] = make([]*FunctionInstance, f.Replicas)
	for i := int32(0); i < f.Replicas; i++ {
		instance := NewFunctionInstance(f, fm.tubeFactory, i)
		fm.functions[f.Name][i] = instance
		go instance.Run()
		if err := <-instance.WaitForReady(); err != nil {
			if err != nil {
				slog.ErrorContext(instance.ctx, "Error starting function instance", err)
			}
			instance.Stop()
			return err
		}
	}
	return nil
}

func (fm *FunctionManager) DeleteFunction(name string) error {
	fm.functionsLock.Lock()
	instances, exist := fm.functions[name]
	if !exist {
		return common.ErrorFunctionNotFound
	}
	delete(fm.functions, name)
	fm.functionsLock.Unlock()
	for _, instance := range instances {
		instance.Stop()
	}
	return nil
}

func (fm *FunctionManager) ListFunctions() (result []string) {
	fm.functionsLock.Lock()
	defer fm.functionsLock.Unlock()
	result = make([]string, len(fm.functions))
	i := 0
	for k := range fm.functions {
		result[i] = k
		i++
	}
	return
}

func (fm *FunctionManager) ProduceEvent(name string, event contube.Record) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, err := fm.tubeFactory.NewSinkTube(ctx, (&contube.SinkQueueConfig{Topic: name}).ToConfigMap())
	if err != nil {
		return err
	}
	c <- event
	return nil
}

func (fm *FunctionManager) ConsumeEvent(name string) (contube.Record, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, err := fm.tubeFactory.NewSourceTube(ctx, (&contube.SourceQueueConfig{Topics: []string{name}, SubName: "consume-" + strconv.Itoa(rand.Int())}).ToConfigMap())
	if err != nil {
		return nil, err
	}
	return <-c, nil
}