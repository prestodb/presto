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

#include <cstddef>
#include "velox/exec/fuzzer/ReferenceQueryRunner.h"

namespace facebook::velox::wave {
struct NimbleReaderFuzzerOptions {
  size_t minNumStreams{1};
  size_t maxNumStreams{10};
  size_t minNumChunks{1};
  size_t maxNumChunks{10};
  size_t minChunkSize{1};
  size_t minNumValues{10};
  size_t maxNumValues{10000};
  double hasNullProbability{0.5};
  double hasFilterProbability{0.75};
  double isFilterProbability{0.5};
  double nullAllowedProbability{0};
  double filterKeepValuesProbability{1.0};
  std::vector<TypePtr> types{INTEGER(), BIGINT(), REAL(), DOUBLE()};
};
void nimbleReaderFuzzer(
    size_t seed,
    NimbleReaderFuzzerOptions options,
    std::unique_ptr<exec::test::ReferenceQueryRunner> referenceQueryRunner);
} // namespace facebook::velox::wave
