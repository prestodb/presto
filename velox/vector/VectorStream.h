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

#include "velox/buffer/Buffer.h"
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

class VectorSerializer {
 public:
  virtual ~VectorSerializer() = default;

  /// Serialize a subset of rows in a vector.
  virtual void append(
      const RowVectorPtr& vector,
      const folly::Range<const IndexRange*>& ranges) = 0;

  /// Serialize all rows in a vector.
  void append(const RowVectorPtr& vector);

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
};

class VectorSerde {
 public:
  virtual ~VectorSerde() = default;

  // Lets the caller pass options to the Serde. This can be extended to add
  // custom options by each of its extended classes.
  struct Options {
    virtual ~Options() {}
  };

  virtual void estimateSerializedSize(
      VectorPtr vector,
      const folly::Range<const IndexRange*>& ranges,
      vector_size_t** sizes) = 0;

  virtual std::unique_ptr<VectorSerializer> createSerializer(
      RowTypePtr type,
      int32_t numRows,
      StreamArena* streamArena,
      const Options* options = nullptr) = 0;

  virtual void deserialize(
      ByteStream* source,
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
      ByteStream* source,
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

  static void estimateSerializedSize(
      VectorPtr vector,
      const folly::Range<const IndexRange*>& ranges,
      vector_size_t** sizes);

  void append(
      const RowVectorPtr& vector,
      const folly::Range<const IndexRange*>& ranges);

  void append(const RowVectorPtr& vector);

  // Writes the contents to 'stream' in wire format.
  void flush(OutputStream* stream);

  // Reads data in wire format. Returns the RowVector in 'result'.
  static void read(
      ByteStream* source,
      velox::memory::MemoryPool* pool,
      RowTypePtr type,
      RowVectorPtr* result,
      const VectorSerde::Options* options = nullptr);

 private:
  std::unique_ptr<VectorSerializer> serializer_;
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
