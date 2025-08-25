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

#include "velox/functions/prestosql/SfmSketchFunctions.h"
#include "velox/functions/lib/sfm/SfmSketch.h"

namespace facebook::velox::functions {

std::string createEmptySfmSketch(
    HashStringAllocator* allocator,
    double epsilon,
    std::optional<int64_t> buckets,
    std::optional<int64_t> precison) {
  using SfmSketch = facebook::velox::functions::sfm::SfmSketch;

  constexpr int64_t kDefaultBuckets = 4096;
  constexpr int64_t kDefaultPrecision = 24;

  int64_t initBuckets = buckets.has_value() ? buckets.value() : kDefaultBuckets;
  int64_t initPrecision =
      precison.has_value() ? precison.value() : kDefaultPrecision;
  SfmSketch sketch(allocator);
  sketch.initialize(
      static_cast<int32_t>(initBuckets), static_cast<int32_t>(initPrecision));
  sketch.enablePrivacy(epsilon);

  // Serialize the sketch.
  auto serializedSize = sketch.serializedSize();
  std::string result(serializedSize, '\0');
  sketch.serialize(result.data());

  return result;
}

} // namespace facebook::velox::functions
