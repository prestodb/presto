/*
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

#include <folly/Random.h>

#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"

namespace facebook::velox {

// Helper class to generate randomized vectors with random (and potentially
// nested) encodings. Use the constructor seed to make it deterministic.
class VectorFuzzer {
 public:
  explicit VectorFuzzer(size_t seed) : rng_(seed) {}

  // Returns a "fuzzed" vector, containing randomized data, nulls, and indices
  // vector (dictionary).
  VectorPtr fuzz(const TypePtr& type, memory::MemoryPool* pool);

  // Returns a flat vector with randomized data and nulls.
  VectorPtr fuzzFlat(const TypePtr& type, memory::MemoryPool* pool);

  // Wraps `vector` using a randomized indices vector, returning a
  // DictionaryVector.
  VectorPtr fuzzDictionary(const VectorPtr& vector, memory::MemoryPool* pool);

 private:
  // Returns true 1/n of times.
  bool oneIn(size_t n) {
    return folly::Random::oneIn(n, rng_);
  }

  size_t batchSize_{100};
  folly::Random::DefaultGenerator rng_;
};

} // namespace facebook::velox
