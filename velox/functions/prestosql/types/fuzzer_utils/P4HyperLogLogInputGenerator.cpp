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

#include "velox/functions/prestosql/types/fuzzer_utils/P4HyperLogLogInputGenerator.h"

#include "velox/functions/prestosql/types/P4HyperLogLogType.h"

namespace facebook::velox::fuzzer {

P4HyperLogLogInputGenerator::P4HyperLogLogInputGenerator(
    const size_t seed,
    const double nullRatio,
    memory::MemoryPool* pool)
    : HyperLogLogInputGenerator(seed, nullRatio, pool, 100) {
  // P4HyperLogLog only supports dense format, unlike HyperLogLog which
  // supports sparse and dense representations. 100 is the minimum number that
  // generate sufficient values to trigger dense format.
  type_ = P4HYPERLOGLOG();
}

} // namespace facebook::velox::fuzzer
