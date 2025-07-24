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
#include "velox/core/QueryConfig.h"
#include "velox/functions/Macros.h"
#include "velox/functions/prestosql/aggregates/sfm/SfmSketch.h"
#include "velox/functions/prestosql/types/SfmSketchType.h"

namespace facebook::velox::functions {

// Helper function to create empty SfmSketch.
std::string createEmptySfmSketch(
    HashStringAllocator* allocator,
    double epsilon,
    std::optional<int64_t> buckets = std::nullopt,
    std::optional<int64_t> precison = std::nullopt);

template <typename T>
struct SfmSketchCardinality {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& /*config*/,
      const arg_type<SfmSketch>* /*input*/) {
    pool_ = memory::memoryManager()->addLeafPool();
    allocator_ = std::make_unique<HashStringAllocator>(pool_.get());
  }

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<SfmSketch>& input) {
    using SfmSketch = facebook::velox::functions::aggregate::SfmSketch;
    result = SfmSketch::deserialize(
                 reinterpret_cast<const char*>(input.data()), allocator_.get())
                 .cardinality();
  }

 private:
  std::shared_ptr<memory::MemoryPool> pool_;
  std::unique_ptr<HashStringAllocator> allocator_;
};

template <typename T>
struct NoisyEmptyApproxSetSfm {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& /*config*/,
      const double* /*epsilon*/,
      const int64_t* /*buckets*/ = nullptr,
      const int64_t* /*precision*/ = nullptr) {
    pool_ = memory::memoryManager()->addLeafPool();
    allocator_ = std::make_unique<HashStringAllocator>(pool_.get());
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<SfmSketch>& result,
      const double& epsilon) {
    auto serialized = createEmptySfmSketch(allocator_.get(), epsilon);
    result.resize(serialized.size());
    // SfmSketch is a binary type, so we can just copy the serialized data.
    memcpy(result.data(), serialized.data(), serialized.size());
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<SfmSketch>& result,
      const double& epsilon,
      const int64_t& buckets) {
    auto serialized = createEmptySfmSketch(allocator_.get(), epsilon, buckets);
    result.resize(serialized.size());
    memcpy(result.data(), serialized.data(), serialized.size());
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<SfmSketch>& result,
      const double& epsilon,
      const int64_t& buckets,
      const int64_t& precision) {
    auto serialized =
        createEmptySfmSketch(allocator_.get(), epsilon, buckets, precision);
    result.resize(serialized.size());
    memcpy(result.data(), serialized.data(), serialized.size());
  }

 private:
  std::shared_ptr<memory::MemoryPool> pool_;
  std::unique_ptr<HashStringAllocator> allocator_;
};

template <typename T>
struct mergeSfmSketchArray {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& /*config*/,
      const arg_type<Array<SfmSketch>>* /*input*/) {
    pool_ = memory::memoryManager()->addLeafPool();
    allocator_ = std::make_unique<HashStringAllocator>(pool_.get());
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<SfmSketch>& result,
      const arg_type<Array<SfmSketch>>& input) {
    if (input.size() == 0) {
      return false;
    }

    using SfmSketch = facebook::velox::functions::aggregate::SfmSketch;

    // Collect all non-null sketches.
    std::vector<const char*> validSketches;
    for (auto i = 0; i < input.size(); ++i) {
      if (input[i].has_value()) {
        const auto& value = input[i].value();
        validSketches.push_back(reinterpret_cast<const char*>(value.data()));
      }
    }

    if (validSketches.empty()) {
      return false;
    }

    auto mergedSketch =
        SfmSketch::deserialize(validSketches[0], allocator_.get());

    for (size_t i = 1; i < validSketches.size(); ++i) {
      mergedSketch.mergeWith(
          SfmSketch::deserialize(validSketches[i], allocator_.get()));
    }

    // Serialize the merged sketch back to result.
    auto serializedSize = mergedSketch.serializedSize();
    result.resize(serializedSize);
    mergedSketch.serialize(result.data());

    return true;
  }

 private:
  std::shared_ptr<memory::MemoryPool> pool_;
  std::unique_ptr<HashStringAllocator> allocator_;
};

} // namespace facebook::velox::functions
