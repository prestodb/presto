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
#include <boost/crc.hpp>
#include "velox/vector/VectorBatch.h"

namespace facebook::velox::batch {

class UnsafeRowKeySerializer : public VectorKeySerializer {
 public:
  BatchSerdeStatus serializeKeys(
      const std::shared_ptr<RowVector>& vector,
      const std::vector<uint64_t>& keys,
      vector_size_t row,
      std::string_view& serializedKeysBuffer,
      std::string_view& serializedKeys,
      size_t& bufferOffset) override;

  virtual ~UnsafeRowKeySerializer() = default;
};

class UnsafeRowVectorSerde : public VectorSerde {
 public:
  UnsafeRowVectorSerde(
      velox::memory::MemoryPool* pool,
      const std::unique_ptr<VectorKeySerializer>& keySerializer = nullptr,
      const std::optional<std::vector<uint64_t>>& keys = std::nullopt)
      : VectorSerde(keySerializer),
        pool_(pool),
        rowBufferPtr_(nullptr),
        rowBuffer_(nullptr, 0),
        keys_{std::move(keys)} {}

  virtual ~UnsafeRowVectorSerde() = default;

  void reset(const std::shared_ptr<RowVector>& vector) override;

  BatchSerdeStatus serializeRow(
      const std::shared_ptr<RowVector>& vector,
      const vector_size_t row,
      std::string_view& serializedKeys,
      std::string_view& serializedRow) override;

  BatchSerdeStatus deserializeVector(
      const std::vector<std::optional<std::string_view>>& values,
      const std::shared_ptr<const RowType> type,
      std::shared_ptr<RowVector>* result) override;

 private:
  // Pool used for memory allocations
  velox::memory::MemoryPool* pool_;
  // Internally allocated buffer to be used by single row serialize
  BufferPtr rowBufferPtr_;
  std::string_view rowBuffer_;
  // List of keys that may need to be serialized along with the row
  const std::optional<std::vector<uint64_t>> keys_;
};

} // namespace facebook::velox::batch
