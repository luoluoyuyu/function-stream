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

package server

import (
	"context"
	"encoding/json"
	"github.com/functionstream/functionstream/common/model"
	"github.com/functionstream/functionstream/lib"
	"github.com/functionstream/functionstream/lib/contube"
	"github.com/functionstream/functionstream/tests"
	"math/rand"
	"strconv"
	"testing"
)

func TestStandaloneBasicFunction(t *testing.T) {

	conf := &lib.Config{
		ListenAddr: "localhost:7301",
		QueueBuilder: func(ctx context.Context, config *lib.Config) (contube.TubeFactory, error) {
			return contube.NewMemoryQueueFactory(ctx), nil
		},
	}
	s := New(conf)
	svrCtx, svrCancel := context.WithCancel(context.Background())
	go s.Run(svrCtx)
	defer func() {
		svrCancel()
	}()

	inputTopic := "test-input-" + strconv.Itoa(rand.Int())
	outputTopic := "test-output-" + strconv.Itoa(rand.Int())

	funcConf := &model.Function{
		Archive:  "../bin/example_basic.wasm",
		Inputs:   []string{inputTopic},
		Output:   outputTopic,
		Name:     "test-func",
		Replicas: 1,
	}
	err := s.manager.StartFunction(funcConf)
	if err != nil {
		t.Fatal(err)
	}

	p := &tests.Person{
		Name:  "rbt",
		Money: 0,
	}
	jsonBytes, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	err = s.manager.ProduceEvent(inputTopic, contube.NewRecordImpl(jsonBytes, func() {
	}))
	if err != nil {
		t.Fatal(err)
	}

	output := make(chan contube.Record)
	go func() {
		event, err := s.manager.ConsumeEvent(outputTopic)
		if err != nil {
			t.Error(err)
			return
		}
		output <- event
	}()

	event := <-output
	var out tests.Person
	err = json.Unmarshal(event.GetPayload(), &out)
	if err != nil {
		t.Error(err)
		return
	}
	if out.Money != 1 {
		t.Errorf("expected 1, got %d", out.Money)
		return
	}
}