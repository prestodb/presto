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

#include "velox/common/memory/HashStringAllocator.h"
#include "velox/functions/lib/sfm/SfmSketch.h"

namespace facebook::velox::functions::sfm {

class SfmSketchAccumulator {
  static constexpr int32_t kDefaultBuckets = 4096;
  static constexpr int32_t kDefaultPrecision = 24;

 public:
  explicit SfmSketchAccumulator(HashStringAllocator* allocator);

  // Constructor with all member variables, used for deserialization.
  explicit SfmSketchAccumulator(
      double epsilon,
      int32_t buckets,
      int32_t precision,
      SfmSketch&& sketch);

  // Initialize the sketch with the given parameters.
  void initialize(
      std::optional<int32_t> buckets = std::nullopt,
      std::optional<int32_t> precision = std::nullopt);

  // Apply privacy and return the sketch.
  const SfmSketch& sketch();

  double epsilon() const {
    return epsilon_;
  }

  void setEpsilon(double epsilon) {
    epsilon_ = epsilon;
  }

  // Check if the sketch is initialized. If not, the group is null or input is
  // empty.
  bool isInitialized() const {
    return sketch_.isInitialized();
  }

  size_t serializedSize() const;

  void serialize(char* outputBuffer) const;

  static SfmSketchAccumulator deserialize(
      const char* inputBuffer,
      HashStringAllocator* allocator);

  // Add a value to the sketch.
  template <typename T>
  void add(T value) {
    VELOX_CHECK(isInitialized(), "Sketch is not set");
    sketch_.add(value);
  }

  // Add a value to the sketch through index and zeros.
  void addIndexAndZeros(int32_t bucketIndex, int32_t zeros);

  // Merge with another accumulator.
  void mergeWith(SfmSketchAccumulator& other);

  // Apply privacy with epsilon and return the estimated cardinality.
  int64_t cardinality();

 private:
  // Epsilon to make the sketch private.
  double epsilon_{-1.0};

  // Buckets of the sketch.
  int32_t buckets_{kDefaultBuckets};

  // Precision of the sketch.
  int32_t precision_{kDefaultPrecision};

  // The sketch to aggregate cardinality.
  SfmSketch sketch_;
};

} // namespace facebook::velox::functions::sfm
