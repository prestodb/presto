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

  virtual void append(
      RowVectorPtr vector,
      const folly::Range<const IndexRange*>& ranges) = 0;

  // Writes the contents to 'stream' in wire format
  virtual void flush(OutputStream* FOLLY_NONNULL stream) = 0;
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
      vector_size_t* FOLLY_NONNULL* FOLLY_NONNULL sizes) = 0;

  virtual std::unique_ptr<VectorSerializer> createSerializer(
      RowTypePtr type,
      int32_t numRows,
      StreamArena* FOLLY_NONNULL streamArena,
      const Options* FOLLY_NULLABLE options = nullptr) = 0;

  virtual void deserialize(
      ByteStream* FOLLY_NONNULL source,
      velox::memory::MemoryPool* FOLLY_NONNULL pool,
      RowTypePtr type,
      RowVectorPtr* FOLLY_NONNULL result,
      const Options* FOLLY_NULLABLE options = nullptr) = 0;
};

bool registerVectorSerde(std::unique_ptr<VectorSerde> serde);

bool isRegisteredVectorSerde();

class VectorStreamGroup : public StreamArena {
 public:
  explicit VectorStreamGroup(memory::MemoryPool* FOLLY_NONNULL pool)
      : StreamArena(pool) {}

  void createStreamTree(
      RowTypePtr type,
      int32_t numRows,
      const VectorSerde::Options* FOLLY_NULLABLE options = nullptr);

  static void estimateSerializedSize(
      VectorPtr vector,
      const folly::Range<const IndexRange*>& ranges,
      vector_size_t* FOLLY_NONNULL* FOLLY_NONNULL sizes);

  void append(
      RowVectorPtr vector,
      const folly::Range<const IndexRange*>& ranges);

  // Writes the contents to 'stream' in wire format.
  void flush(OutputStream* FOLLY_NONNULL stream);

  // Reads data in wire format. Returns the RowVector in 'result'.
  static void read(
      ByteStream* FOLLY_NONNULL source,
      velox::memory::MemoryPool* FOLLY_NONNULL pool,
      RowTypePtr type,
      RowVectorPtr* FOLLY_NONNULL result,
      const VectorSerde::Options* FOLLY_NULLABLE options = nullptr);

 private:
  std::unique_ptr<VectorSerializer> serializer_;
};

} // namespace facebook::velox
