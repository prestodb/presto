/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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

#pragma once

#include "velox/experimental/wave/exec/HashGen.h"
#include "velox/experimental/wave/exec/ToWave.h"

namespace facebook::velox::wave {

void makeAggregateOps(
    CompileState& state,
    const AggregateProbe& probe,
    bool forRead);

void makeAggregateProbe(
    CompileState& state,
    const AggregateProbe& probe,
    int32_t syncLabel);

void makeReadAggregation(
    CompileState& state,
    const ReadAggregation& read,
    int32_t syncLabel);

} // namespace facebook::velox::wave
