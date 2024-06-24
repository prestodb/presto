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

#include "folly/Executor.h"
#include "velox/common/memory/Memory.h"
#include "velox/dwio/common/ColumnSelector.h"
#include "velox/dwio/common/TypeWithId.h"
#include "velox/dwio/common/compression/Compression.h"
#include "velox/dwio/dwrf/common/ByteRLE.h"
#include "velox/dwio/dwrf/common/wrap/dwrf-proto-wrapper.h"
#include "velox/dwio/dwrf/reader/EncodingContext.h"
#include "velox/dwio/dwrf/reader/StreamLabels.h"
#include "velox/dwio/dwrf/reader/StripeStream.h"
#include "velox/vector/BaseVector.h"

namespace facebook::velox::dwrf {

// Foward declaration
class ColumnReader;

class ColumnReaderFactory {
 public:
  virtual ~ColumnReaderFactory() = default;
  virtual std::unique_ptr<ColumnReader> build(
      const std::shared_ptr<const dwio::common::TypeWithId>& requestedType,
      const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
      StripeStreams& stripe,
      const StreamLabels& streamLabels,
      folly::Executor* executor,
      size_t decodingParallelismFactor,
      FlatMapContext flatMapContext = {});

  static ColumnReaderFactory& defaultFactory();
};

/**
 * The interface for reading ORC data types.
 */
class ColumnReader {
 protected:
  ColumnReader(
      memory::MemoryPool& memoryPool,
      const std::shared_ptr<const dwio::common::TypeWithId>& type)
      : notNullDecoder_{},
        fileType_{type},
        memoryPool_{memoryPool},
        flatMapContext_{} {}

  // Reads nulls, if any. Sets '*nulls' to nullptr if void
  // the reader has no nulls and there are no incoming
  //          nulls.Takes 'nulls' from 'result' if '*result' is non -
  //      null.Otherwise ensures that 'nulls' has a buffer of sufficient
  //          size and uses this.
  void readNulls(
      vector_size_t numValues,
      const uint64_t* incomingNulls,
      VectorPtr* result,
      BufferPtr& nulls);

  // Shorthand for long form of readNulls for use in next().
  BufferPtr readNulls(
      vector_size_t numValues,
      VectorPtr& result,
      const uint64_t* incomingNulls);

  std::unique_ptr<ByteRleDecoder> notNullDecoder_;
  const std::shared_ptr<const dwio::common::TypeWithId> fileType_;
  memory::MemoryPool& memoryPool_;
  FlatMapContext flatMapContext_;

 public:
  ColumnReader(
      std::shared_ptr<const dwio::common::TypeWithId> nodeId,
      StripeStreams& stripe,
      const StreamLabels& streamLabels,
      FlatMapContext flatMapContext = {});

  virtual ~ColumnReader() = default;

  /**
   * Skip number of specified rows.
   * @param numValues the number of values to skip
   * @return the number of non-null values skipped
   */
  virtual uint64_t skip(uint64_t numValues);

  /**
   * Read the next group of values into a RowVector.
   * @param numValues the number of values to read
   * @param vector to read into
   */
  virtual void next(
      uint64_t numValues,
      VectorPtr& result,
      const uint64_t* nulls = nullptr) = 0;

  // Return list of strides/rowgroups that can be skipped (based on statistics).
  // Stride indices are monotonically increasing.
  virtual std::vector<uint32_t> filterRowGroups(
      uint64_t /*rowGroupSize*/,
      const StatsContext& /* context */) const {
    static const std::vector<uint32_t> kEmpty;
    return kEmpty;
  }

  // Sets the streams of this and child readers to the first row of
  // the row group at 'index'. This advances readers and touches the
  // actual data, unlike setRowGroup().
  virtual void seekToRowGroup(uint32_t /*index*/) {
    VELOX_NYI();
  }

  virtual bool isFlatMap() const {
    return false;
  }

  /**
   * Create a reader for the given stripe.
   */
  static std::unique_ptr<ColumnReader> build(
      const std::shared_ptr<const dwio::common::TypeWithId>& requestedType,
      const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
      StripeStreams& stripe,
      const StreamLabels& streamLabels,
      folly::Executor* executor,
      size_t decodingParallelismFactor,
      FlatMapContext flatMapContext = {},
      ColumnReaderFactory& factory = ColumnReaderFactory::defaultFactory());
};

namespace detail {

template <typename T>
inline void ensureCapacity(
    BufferPtr& data,
    size_t capacity,
    velox::memory::MemoryPool* pool) {
  if (!data || !data->unique() ||
      data->capacity() < BaseVector::byteSize<T>(capacity)) {
    data = AlignedBuffer::allocate<T>(capacity, pool);
  }
}

template <typename T>
inline T* resetIfWrongVectorType(VectorPtr& result) {
  if (result) {
    auto casted = result->as<T>();
    // We only expect vector to be used by a single thread.
    if (casted && result.use_count() == 1) {
      return casted;
    }
    result.reset();
  }
  return nullptr;
}

template <typename... T>
inline void resetIfNotWritable(VectorPtr& result, T&... buffer) {
  // The result vector and the buffer both hold reference, so refCount is at
  // least 2
  auto resetIfShared = [](auto& buffer) {
    const bool reset = buffer->refCount() > 2;
    if (reset) {
      buffer.reset();
    }
    return reset;
  };

  if ((... || resetIfShared(buffer))) {
    result.reset();
  }
}

// Helper method to build timestamps based on nulls/seconds/nanos
void fillTimestamps(
    Timestamp* timestamps,
    const uint64_t* nulls,
    const int64_t* seconds,
    const uint64_t* nanos,
    vector_size_t numValues);

} // namespace detail
} // namespace facebook::velox::dwrf
