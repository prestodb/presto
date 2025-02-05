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

#include "velox/experimental/wave/common/Cuda.h"
#include "velox/experimental/wave/exec/ExprKernel.h"

namespace facebook::velox::wave {

/// A stream for invoking ExprKernel.
class WaveKernelStream : public Stream {
 public:
  /// Sets up or updates an aggregation.
  void setupAggregation(
      AggregationControl& op,
      int32_t entryPoint,
      CompiledKernel* kernel);
};

} // namespace facebook::velox::wave
