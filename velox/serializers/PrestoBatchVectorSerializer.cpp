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

#include "velox/serializers/PrestoBatchVectorSerializer.h"

#include "velox/serializers/PrestoSerializerEstimationUtils.h"
#include "velox/serializers/PrestoSerializerSerializationUtils.h"
#include "velox/serializers/VectorStream.h"

namespace facebook::velox::serializer::presto::detail {
void PrestoBatchVectorSerializer::serialize(
    const RowVectorPtr& vector,
    const folly::Range<const IndexRange*>& ranges,
    Scratch& scratch,
    OutputStream* stream) {
  VELOX_CHECK_NOT_NULL(vector, "Vector to serialize is null.");
  VELOX_CHECK_NOT_NULL(stream, "Stream to serialize out to is null.");

#ifndef NDEBUG
  for (int i = 0; i < ranges.size(); i++) {
    VELOX_CHECK_GE(ranges[i].begin, 0, "Invalid range at index {}", i);
    VELOX_CHECK_LE(
        ranges[i].begin + ranges[i].size,
        vector->size(),
        "Invalid range at index {}",
        i);
  }
#endif

  SCOPE_EXIT {
    inUse.store(false);
  };

  VELOX_CHECK(
      !inUse.exchange(true),
      "PrestoBatchVectorSerializer::serialize being called concurrently on the same object.");

  common::testutil::TestValue::adjust(
      "facebook::velox::serializers::PrestoBatchVectorSerializer::serialize",
      this);

  const auto numRows = rangesTotalSize(ranges);
  const auto rowType = vector->type();
  const auto numChildren = vector->childrenSize();

  StreamArena arena(pool_);
  std::vector<VectorStream> streams;
  streams.reserve(numChildren);
  for (int i = 0; i < numChildren; i++) {
    streams.emplace_back(
        rowType->childAt(i),
        std::nullopt,
        vector->childAt(i),
        &arena,
        numRows,
        opts_);

    if (numRows > 0) {
      serializeColumn(vector->childAt(i), ranges, &streams[i], scratch);
    }
  }

  flushStreams(
      streams, numRows, arena, *codec_, opts_.minCompressionRatio, stream);
}

void PrestoBatchVectorSerializer::estimateSerializedSizeImpl(
    const VectorPtr& vector,
    const folly::Range<const IndexRange*>& ranges,
    vector_size_t** sizes,
    Scratch& scratch) {
  switch (vector->encoding()) {
    case VectorEncoding::Simple::FLAT:
      VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH_ALL(
          estimateFlatSerializedSize,
          vector->typeKind(),
          vector.get(),
          ranges,
          sizes);
      break;
    case VectorEncoding::Simple::CONSTANT:
      VELOX_DYNAMIC_TYPE_DISPATCH_ALL(
          estimateConstantSerializedSize,
          vector->typeKind(),
          vector,
          ranges,
          sizes,
          scratch);
      break;
    case VectorEncoding::Simple::DICTIONARY:
      VELOX_DYNAMIC_TYPE_DISPATCH_ALL(
          estimateDictionarySerializedSize,
          vector->typeKind(),
          vector,
          ranges,
          sizes,
          scratch);
      break;
    case VectorEncoding::Simple::ROW: {
      if (!vector->mayHaveNulls()) {
        // Add the size of the offsets in the Row encoding.
        for (int32_t i = 0; i < ranges.size(); ++i) {
          *sizes[i] += ranges[i].size * sizeof(int32_t);
        }

        auto rowVector = vector->as<RowVector>();
        auto& children = rowVector->children();
        for (auto& child : children) {
          if (child) {
            estimateSerializedSizeImpl(child, ranges, sizes, scratch);
          }
        }

        break;
      }

      std::vector<IndexRange> childRanges;
      std::vector<vector_size_t*> childSizes;
      for (int32_t i = 0; i < ranges.size(); ++i) {
        // Add the size of the nulls bit mask.
        *sizes[i] += bits::nbytes(ranges[i].size);

        auto begin = ranges[i].begin;
        auto end = begin + ranges[i].size;
        for (auto offset = begin; offset < end; ++offset) {
          // Add the size of the offset.
          *sizes[i] += sizeof(int32_t);
          if (!vector->isNullAt(offset)) {
            childRanges.push_back(IndexRange{offset, 1});
            childSizes.push_back(sizes[i]);
          }
        }
      }

      auto rowVector = vector->as<RowVector>();
      auto& children = rowVector->children();
      for (auto& child : children) {
        if (child) {
          estimateSerializedSizeImpl(
              child,
              folly::Range(childRanges.data(), childRanges.size()),
              childSizes.data(),
              scratch);
        }
      }

      break;
    }
    case VectorEncoding::Simple::MAP: {
      auto mapVector = vector->as<MapVector>();
      std::vector<IndexRange> childRanges;
      std::vector<vector_size_t*> childSizes;
      expandRepeatedRanges(
          mapVector,
          mapVector->rawOffsets(),
          mapVector->rawSizes(),
          ranges,
          sizes,
          &childRanges,
          &childSizes);
      estimateSerializedSizeImpl(
          mapVector->mapKeys(), childRanges, childSizes.data(), scratch);
      estimateSerializedSizeImpl(
          mapVector->mapValues(), childRanges, childSizes.data(), scratch);
      break;
    }
    case VectorEncoding::Simple::ARRAY: {
      auto arrayVector = vector->as<ArrayVector>();
      std::vector<IndexRange> childRanges;
      std::vector<vector_size_t*> childSizes;
      expandRepeatedRanges(
          arrayVector,
          arrayVector->rawOffsets(),
          arrayVector->rawSizes(),
          ranges,
          sizes,
          &childRanges,
          &childSizes);
      estimateSerializedSizeImpl(
          arrayVector->elements(), childRanges, childSizes.data(), scratch);
      break;
    }
    case VectorEncoding::Simple::LAZY:
      estimateSerializedSizeImpl(
          vector->as<LazyVector>()->loadedVectorShared(),
          ranges,
          sizes,
          scratch);
      break;
    default:
      VELOX_UNSUPPORTED("Unsupported vector encoding {}", vector->encoding());
  }
}
} // namespace facebook::velox::serializer::presto::detail
