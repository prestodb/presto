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

#include <folly/Range.h>
#include "velox/buffer/Buffer.h"
#include "velox/common/base/RuntimeMetrics.h"
#include "velox/common/base/Scratch.h"
#include "velox/common/compression/Compression.h"
#include "velox/common/memory/ByteStream.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/memory/MemoryAllocator.h"
#include "velox/common/memory/StreamArena.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox {

class ByteStream;

struct IndexRange {
  vector_size_t begin;
  vector_size_t size;
};

/// Serializer that can iteratively build up a buffer of serialized rows from
/// one or more RowVectors.
///
/// Uses successive calls to `append` to add more rows to the serialization
/// buffer.  Then call `flush` to write the aggregate serialized data to an
/// OutputStream.
class IterativeVectorSerializer {
 public:
  virtual ~IterativeVectorSerializer() = default;

  /// Serialize a subset of rows in a vector.
  virtual void append(
      const RowVectorPtr& vector,
      const folly::Range<const IndexRange*>& ranges,
      Scratch& scratch) = 0;

  virtual void append(
      const RowVectorPtr& vector,
      const folly::Range<const IndexRange*>& ranges) {
    Scratch scratch;
    append(vector, ranges, scratch);
  }

  virtual void append(
      const RowVectorPtr& vector,
      const folly::Range<const vector_size_t*>& rows,
      Scratch& scratch) {
    VELOX_UNSUPPORTED();
  }

  /// Serialize all rows in a vector.
  void append(const RowVectorPtr& vector);

  // True if supports append with folly::Range<vector_size_t*>.
  virtual bool supportsAppendRows() const {
    return false;
  }

  /// Returns the maximum serialized size of the data previously added via
  /// 'append' methods. Can be used to allocate buffer of exact or maximum size
  /// before calling 'flush'.
  /// Returns the exact serialized size when data is not compressed.
  /// Returns the maximum serialized size when data is compressed.
  ///
  /// Usage
  /// append(vector, ranges);
  /// size_t size = maxSerializedSize();
  /// OutputStream* stream = allocateBuffer(size);
  /// flush(stream);
  virtual size_t maxSerializedSize() const = 0;

  /// Write serialized data to 'stream'.
  virtual void flush(OutputStream* stream) = 0;

  /// Resets 'this' to post construction state.
  virtual void clear() {
    VELOX_UNSUPPORTED("clear");
  }

  /// Returns serializer-dependent counters, e.g. about compression, data
  /// distribution, encoding etc.
  virtual std::unordered_map<std::string, RuntimeCounter> runtimeStats() {
    return {};
  }
};

/// Serializer that writes a subset of rows from a single RowVector to the
/// OutputStream.
///
/// Each serialize() call serializes the specified range(s) of `vector` and
/// write them to the output stream.
class BatchVectorSerializer {
 public:
  virtual ~BatchVectorSerializer() = default;

  /// Serializes a subset of rows in a vector.
  virtual void serialize(
      const RowVectorPtr& vector,
      const folly::Range<const IndexRange*>& ranges,
      Scratch& scratch,
      OutputStream* stream) = 0;

  virtual void serialize(
      const RowVectorPtr& vector,
      const folly::Range<const IndexRange*>& ranges,
      OutputStream* stream) {
    Scratch scratch;
    serialize(vector, ranges, scratch, stream);
  }

  /// Serializes all rows in a vector.
  void serialize(const RowVectorPtr& vector, OutputStream* stream);
};

class VectorSerde {
 public:
  virtual ~VectorSerde() = default;

  // Lets the caller pass options to the Serde. This can be extended to add
  // custom options by each of its extended classes.
  struct Options {
    Options() = default;

    explicit Options(common::CompressionKind _compressionKind)
        : compressionKind(_compressionKind) {}

    virtual ~Options() = default;

    common::CompressionKind compressionKind{
        common::CompressionKind::CompressionKind_NONE};
  };

  /// Adds the serialized size of vector at 'rows[i]' to '*sizes[i]'.
  virtual void estimateSerializedSize(
      const BaseVector* /*vector*/,
      folly::Range<const vector_size_t*> rows,
      vector_size_t** sizes,
      Scratch& scratch) {
    VELOX_UNSUPPORTED();
  }

  /// Adds the serialized sizes of the rows of 'vector' in 'ranges[i]' to
  /// '*sizes[i]'.
  virtual void estimateSerializedSize(
      const BaseVector* /*vector*/,
      const folly::Range<const IndexRange*>& ranges,
      vector_size_t** sizes,
      Scratch& scratch) {
    VELOX_UNSUPPORTED();
  }

  virtual void estimateSerializedSize(
      const BaseVector* vector,
      const folly::Range<const IndexRange*>& ranges,
      vector_size_t** sizes) {
    Scratch scratch;
    estimateSerializedSize(vector, ranges, sizes, scratch);
  }

  /// Creates a Vector Serializer that iteratively builds up a buffer of
  /// serialized rows from one or more RowVectors via append, and then writes to
  /// an OutputSteam via flush.
  ///
  /// This is more appropriate if the use case involves many small writes, e.g.
  /// partitioning a RowVector across multiple destinations.
  virtual std::unique_ptr<IterativeVectorSerializer> createIterativeSerializer(
      RowTypePtr type,
      int32_t numRows,
      StreamArena* streamArena,
      const Options* options = nullptr) = 0;

  /// Creates a Vector Serializer that writes a subset of rows from a single
  /// RowVector to the OutputStream via a single serialize API.
  ///
  /// This is more appropriate if the use case involves large writes, e.g.
  /// sending an entire RowVector to a particular destination.
  virtual std::unique_ptr<BatchVectorSerializer> createBatchSerializer(
      memory::MemoryPool* pool,
      const Options* options = nullptr);

  virtual void deserialize(
      ByteInputStream* source,
      velox::memory::MemoryPool* pool,
      RowTypePtr type,
      RowVectorPtr* result,
      const Options* options = nullptr) = 0;

  /// Returns true if implements 'deserialize' API with 'resultOffset' to allow
  /// for appending deserialized data to an existing vector.
  virtual bool supportsAppendInDeserialize() const {
    return false;
  }

  /// Deserializes data from 'source' and appends to 'result' vector starting at
  /// 'resultOffset'.
  /// @param result Result vector to append new data to. Can be null only if
  /// 'resultOffset' is zero.
  /// @param resultOffset Must be greater than or equal to zero. If > 0, must be
  /// less than or equal to the size of 'result'.
  virtual void deserialize(
      ByteInputStream* source,
      velox::memory::MemoryPool* pool,
      RowTypePtr type,
      RowVectorPtr* result,
      vector_size_t resultOffset,
      const Options* options = nullptr) {
    if (resultOffset == 0) {
      deserialize(source, pool, type, result, options);
      return;
    }
    VELOX_UNSUPPORTED();
  }
};

/// Register/deregister the "default" vector serde.
void registerVectorSerde(std::unique_ptr<VectorSerde> serdeToRegister);
void deregisterVectorSerde();

/// Check if a "default" vector serde has been registered.
bool isRegisteredVectorSerde();

/// Get the "default" vector serde, if one has been registered.
VectorSerde* getVectorSerde();

/// Register/deregister a named vector serde. `serdeName` is a handle that
/// allows users to register multiple serde formats.
void registerNamedVectorSerde(
    std::string_view serdeName,
    std::unique_ptr<VectorSerde> serdeToRegister);
void deregisterNamedVectorSerde(std::string_view serdeName);

/// Check if a named vector serde has been registered with `serdeName` as a
/// handle.
bool isRegisteredNamedVectorSerde(std::string_view serdeName);

/// Get the vector serde identified by `serdeName`. Throws if not found.
VectorSerde* getNamedVectorSerde(std::string_view serdeName);

class VectorStreamGroup : public StreamArena {
 public:
  /// If `serde` is not specified, fallback to the default registered.
  explicit VectorStreamGroup(
      memory::MemoryPool* FOLLY_NONNULL pool,
      VectorSerde* serde = nullptr)
      : StreamArena(pool),
        serde_(serde != nullptr ? serde : getVectorSerde()) {}

  void createStreamTree(
      RowTypePtr type,
      int32_t numRows,
      const VectorSerde::Options* options = nullptr);

  /// Increments sizes[i] for each ith row in 'rows' in 'vector'.
  static void estimateSerializedSize(
      const BaseVector* vector,
      folly::Range<const vector_size_t*> rows,
      vector_size_t** sizes,
      Scratch& scratch);

  static void estimateSerializedSize(
      const BaseVector* vector,
      const folly::Range<const IndexRange*>& ranges,
      vector_size_t** sizes,
      Scratch& scratch);

  static inline void estimateSerializedSize(
      const BaseVector* vector,
      const folly::Range<const IndexRange*>& ranges,
      vector_size_t** sizes) {
    Scratch scratch;
    estimateSerializedSize(vector, ranges, sizes, scratch);
  }

  void append(
      const RowVectorPtr& vector,
      const folly::Range<const IndexRange*>& ranges,
      Scratch& scratch);

  void append(
      const RowVectorPtr& vector,
      const folly::Range<const IndexRange*>& ranges) {
    Scratch scratch;
    append(vector, ranges, scratch);
  }

  void append(
      const RowVectorPtr& vector,
      const folly::Range<const vector_size_t*>& rows,
      Scratch& scratch);

  void append(const RowVectorPtr& vector);

  // Writes the contents to 'stream' in wire format.
  void flush(OutputStream* stream);

  // Reads data in wire format. Returns the RowVector in 'result'.
  static void read(
      ByteInputStream* source,
      velox::memory::MemoryPool* pool,
      RowTypePtr type,
      RowVectorPtr* result,
      const VectorSerde::Options* options = nullptr);

  void clear() override {
    StreamArena::clear();
    serializer_->clear();
  }

  /// Returns serializer-dependent counters, e.g. about compression, data
  /// distribution, encoding etc.
  std::unordered_map<std::string, RuntimeCounter> runtimeStats() {
    if (!serializer_) {
      return {};
    }
    return serializer_->runtimeStats();
  }

 private:
  std::unique_ptr<IterativeVectorSerializer> serializer_;
  VectorSerde* serde_{nullptr};
};

/// Convenience function to serialize a single rowVector into an IOBuf using the
/// registered serde object.
folly::IOBuf rowVectorToIOBuf(
    const RowVectorPtr& rowVector,
    memory::MemoryPool& pool,
    VectorSerde* serde = nullptr);

/// Same as above but serializes up until row `rangeEnd`.
folly::IOBuf rowVectorToIOBuf(
    const RowVectorPtr& rowVector,
    vector_size_t rangeEnd,
    memory::MemoryPool& pool,
    VectorSerde* serde = nullptr);

/// Convenience function to deserialize an IOBuf into a rowVector. If `serde` is
/// nullptr, use the default installed serializer.
RowVectorPtr IOBufToRowVector(
    const folly::IOBuf& ioBuf,
    const RowTypePtr& outputType,
    memory::MemoryPool& pool,
    VectorSerde* serde = nullptr);

} // namespace facebook::velox
