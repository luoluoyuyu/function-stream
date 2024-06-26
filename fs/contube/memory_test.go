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

package contube

import (
	"context"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestMemoryTube(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())
	tubeFactory := NewMemoryQueueFactory(ctx)
	memoryQueueFactory := tubeFactory.(*MemoryQueueFactory)

	var wg sync.WaitGroup
	var events []Record

	topics := []string{"topic1", "topic2", "topic3"}
	source, err := memoryQueueFactory.NewSourceTube(ctx, (&SourceQueueConfig{Topics: topics,
		SubName: "consume-" + strconv.Itoa(rand.Int())}).ToConfigMap())
	if err != nil {
		t.Fatal(err)
	}

	for i, v := range topics {
		wg.Add(1)
		sink, err := memoryQueueFactory.NewSinkTube(ctx, (&SinkQueueConfig{Topic: v}).ToConfigMap())
		if err != nil {
			t.Fatal(err)
		}
		go func(i int) {
			defer wg.Done()
			defer close(sink)
			sink <- NewRecordImpl([]byte{byte(i + 1)}, func() {})
		}(i)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case event := <-source:
				events = append(events, event)
				if len(events) == len(topics) {
					return
				}
			default:
				continue
			}
		}
	}()

	wg.Wait()
	cancel()

	// Give enough time to ensure that the goroutine execution within NewSource Tube and NewSinkTube is complete and
	// the released queue is successful.
	time.Sleep(100 * time.Millisecond)

	// assert the memoryQueueFactory.queues is empty.
	memoryQueueFactory.mu.Lock()
	if len(memoryQueueFactory.queues) != 0 {
		t.Fatal("MemoryQueueFactory.queues is not empty")
	}
	memoryQueueFactory.mu.Unlock()

}
