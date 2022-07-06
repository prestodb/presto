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

#include "velox/expression/EvalCtx.h"

namespace facebook::velox::exec {

enum class BooleanMix { kAllTrue, kAllFalse, kAllNull, kMixNonNull, kMix };

BooleanMix getFlatBool(
    BaseVector* vector,
    const SelectivityVector& activeRows,
    EvalCtx& context,
    BufferPtr* tempValues,
    BufferPtr* tempNulls,
    bool mergeNullsToValues,
    const uint64_t** valuesOut,
    const uint64_t** nullsOut);
} // namespace facebook::velox::exec
