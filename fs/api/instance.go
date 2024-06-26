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

package api

import (
	"github.com/functionstream/function-stream/common"
	"github.com/functionstream/function-stream/common/model"
	"github.com/functionstream/function-stream/fs/contube"
	"golang.org/x/net/context"
)

type FunctionInstance interface {
	Context() context.Context
	FunctionContext() FunctionContext
	Definition() *model.Function
	Index() int32
	Stop()
	Run(runtime FunctionRuntime, sources []<-chan contube.Record, sink chan<- contube.Record)
	Logger() *common.Logger
}

type FunctionInstanceFactory interface {
	NewFunctionInstance(f *model.Function, funcCtx FunctionContext, i int32, logger *common.Logger) FunctionInstance
}
