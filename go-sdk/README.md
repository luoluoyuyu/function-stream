<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# Function Stream Go SDK

This module provides a production-oriented Go SDK for Function Stream WASM processors.

## Package

- Module: `github.com/functionstream/function-stream/go-sdk`
- Entry API: `fssdk.Run(driver)`

## Driver Contract

Implement the `fssdk.Driver` interface, or embed `fssdk.BaseDriver` and override only required methods.

Core callbacks:

- `Init`
- `Process`
- `ProcessWatermark`
- `TakeCheckpoint`
- `CheckHeartbeat`
- `Close`
- `Exec`
- `Custom`

## Context Contract

Use `fssdk.Context` inside callbacks:

- `Emit`
- `EmitWatermark`
- `GetOrCreateStore`
- `Config`

## Store Contract

Use `fssdk.Store` for state operations:

- Byte KV APIs (`PutState`, `GetState`, `DeleteState`, `ListStates`)
- Complex-key APIs (`Put`, `Get`, `Delete`, `Merge`, `DeletePrefix`, `ListComplex`, `ScanComplex`)

## Build Model

Run from project root:

```bash
make -C go-sdk build
```

Targets:

- `make -C go-sdk env`
- `make -C go-sdk wit`
- `make -C go-sdk bindings`
- `make -C go-sdk build`
- `make -C go-sdk clean`

`wit` copies `wit/processor.wit` from project root and resolves dependencies into `go-sdk/wit`.
`bindings` generates Go code into `go-sdk/bindings`.
