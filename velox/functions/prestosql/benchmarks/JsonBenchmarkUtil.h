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

#include <folly/Benchmark.h>

namespace facebook::velox::functions::prestosql {

#define ISJSONSCALAR_BENCHMARK_NAMED_PARAM(type, name, iter, jsonSize) \
  BENCHMARK_RELATIVE_NAMED_PARAM(                                      \
      name, type##_i_##iter##_jSize_##jsonSize, iter, #jsonSize)

#define ISJSONSCALAR_BENCHMARK_NAMED_PARAM_TWO_FUNCS(             \
    type, func1, func2, iter, jsonSize)                           \
  ISJSONSCALAR_BENCHMARK_NAMED_PARAM(type, func1, iter, jsonSize) \
  ISJSONSCALAR_BENCHMARK_NAMED_PARAM(type, func2, iter, jsonSize)

} // namespace facebook::velox::functions::prestosql
