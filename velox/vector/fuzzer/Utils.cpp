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

#include "velox/vector/fuzzer/Utils.h"

namespace facebook::velox::generator_spec_utils {

bool coinToss(FuzzerGenerator& rng, double threshold) {
  static std::uniform_real_distribution<> dist(0.0, 1.0);
  return dist(rng) < threshold;
}

BufferPtr generateNullsBuffer(
    FuzzerGenerator& rng,
    memory::MemoryPool* pool,
    vector_size_t vectorSize,
    double nullProbability) {
  NullsBuilder builder{vectorSize, pool};
  for (size_t i = 0; i < vectorSize; ++i) {
    if (coinToss(rng, nullProbability)) {
      builder.setNull(i);
    }
  }
  return builder.build();
}

} // namespace facebook::velox::generator_spec_utils
