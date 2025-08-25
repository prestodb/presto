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

#include "velox/functions/lib/sfm/SfmSketchAccumulator.h"
#include "velox/common/base/IOUtils.h"

namespace facebook::velox::functions::sfm {
SfmSketchAccumulator::SfmSketchAccumulator(HashStringAllocator* allocator)
    : sketch_(allocator) {}

SfmSketchAccumulator::SfmSketchAccumulator(
    double epsilon,
    int32_t buckets,
    int32_t precision,
    SfmSketch&& sketch)
    : epsilon_{epsilon},
      buckets_{buckets},
      precision_{precision},
      sketch_{std::move(sketch)} {}

void SfmSketchAccumulator::initialize(
    std::optional<int32_t> buckets,
    std::optional<int32_t> precision) {
  const auto initBuckets = buckets.value_or(kDefaultBuckets);
  const auto initPrecision = precision.value_or(kDefaultPrecision);
  sketch_.initialize(initBuckets, initPrecision);
  buckets_ = initBuckets;
  precision_ = initPrecision;
}

const SfmSketch& SfmSketchAccumulator::sketch() {
  VELOX_CHECK(isInitialized(), "Sketch is not set");
  if (!sketch_.privacyEnabled() && epsilon_ > 0) {
    sketch_.enablePrivacy(epsilon_);
  }
  return sketch_;
}

size_t SfmSketchAccumulator::serializedSize() const {
  return sizeof(epsilon_) + sizeof(buckets_) + sizeof(precision_) +
      sketch_.serializedSize();
}

void SfmSketchAccumulator::serialize(char* outputBuffer) const {
  common::OutputByteStream stream(outputBuffer);
  stream.appendOne(epsilon_);
  stream.appendOne(buckets_);
  stream.appendOne(precision_);
  sketch_.serialize(outputBuffer + stream.offset());
}

SfmSketchAccumulator SfmSketchAccumulator::deserialize(
    const char* inputBuffer,
    HashStringAllocator* allocator) {
  common::InputByteStream stream(inputBuffer);
  const auto epsilon = stream.read<double>();
  const auto buckets = stream.read<int32_t>();
  const auto precision = stream.read<int32_t>();
  return SfmSketchAccumulator{
      epsilon,
      buckets,
      precision,
      SfmSketch::deserialize(inputBuffer + stream.offset(), allocator)};
}

void SfmSketchAccumulator::mergeWith(SfmSketchAccumulator& other) {
  if (!other.isInitialized()) {
    return;
  }
  if (!isInitialized()) {
    epsilon_ = other.epsilon_;
    this->initialize(other.buckets_, other.precision_);
  }
  sketch_.mergeWith(other.sketch_);
}

void SfmSketchAccumulator::addIndexAndZeros(
    int32_t bucketIndex,
    int32_t zeros) {
  VELOX_CHECK(isInitialized(), "Sketch is not set");
  sketch_.addIndexAndZeros(bucketIndex, zeros);
}

int64_t SfmSketchAccumulator::cardinality() {
  VELOX_CHECK(
      isInitialized(), "Cardinality is not available for empty sketch.");
  // Add noise to the sketch before computing cardinality.
  if (!sketch_.privacyEnabled() && epsilon_ > 0) {
    sketch_.enablePrivacy(epsilon_);
  }
  return sketch_.cardinality();
}
} // namespace facebook::velox::functions::sfm
