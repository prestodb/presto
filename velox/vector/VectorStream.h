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
#include "velox/common/memory/MappedMemory.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/memory/StreamArena.h"
#include "velox/type/Type.h"
#include "velox/vector/SelectivityVector.h"

namespace facebook::velox {

class BaseVector;
class ByteStream;
class RowVector;

struct IndexRange {
  vector_size_t begin;
  vector_size_t size;
};

class VectorSerializer {
 public:
  virtual ~VectorSerializer() = default;

  virtual void append(
      std::shared_ptr<RowVector> vector,
      const folly::Range<const IndexRange*>& ranges) = 0;

  // Writes the contents to 'stream' in wire format
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
      std::shared_ptr<BaseVector> vector,
      const folly::Range<const IndexRange*>& ranges,
      vector_size_t** sizes) = 0;

  virtual std::unique_ptr<VectorSerializer> createSerializer(
      std::shared_ptr<const RowType> type,
      int32_t numRows,
      StreamArena* streamArena,
      const Options* options = nullptr) = 0;

  virtual void deserialize(
      ByteStream* source,
      velox::memory::MemoryPool* pool,
      std::shared_ptr<const RowType> type,
      std::shared_ptr<RowVector>* result,
      const Options* options = nullptr) = 0;
};

bool registerVectorSerde(std::unique_ptr<VectorSerde> serde);

bool isRegisteredVectorSerde();

#define _VELOX_REGISTER_VECTOR_SERDE_NAME(serde) registerVectorSerde_##serde

#define VELOX_DECLARE_VECTOR_SERDE(serde)             \
  void _VELOX_REGISTER_VECTOR_SERDE_NAME(serde)() {   \
    registerVectorSerde((std::make_unique<serde>())); \
  }

#define VELOX_REGISTER_VECTOR_SERDE(serde)                  \
  {                                                         \
    extern void _VELOX_REGISTER_VECTOR_SERDE_NAME(serde)(); \
    _VELOX_REGISTER_VECTOR_SERDE_NAME(serde)();             \
  }

class VectorStreamGroup : public StreamArena {
 public:
  explicit VectorStreamGroup(memory::MappedMemory* mappedMemory)
      : StreamArena(mappedMemory) {}

  void createStreamTree(
      std::shared_ptr<const RowType> type,
      int32_t numRows,
      const VectorSerde::Options* options = nullptr);

  static void estimateSerializedSize(
      std::shared_ptr<BaseVector> vector,
      const folly::Range<const IndexRange*>& ranges,
      vector_size_t** sizes);

  void append(
      std::shared_ptr<RowVector> vector,
      const folly::Range<const IndexRange*>& ranges);

  // Writes the contents to 'stream' in wire format.
  void flush(OutputStream* stream);

  // Reads data in wire format. Returns the RowVector in 'result'.
  static void read(
      ByteStream* source,
      velox::memory::MemoryPool* pool,
      std::shared_ptr<const RowType> type,
      std::shared_ptr<RowVector>* result,
      const VectorSerde::Options* options = nullptr);

 private:
  std::unique_ptr<VectorSerializer> serializer_;
};

} // namespace facebook::velox
