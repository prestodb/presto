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
#include "velox/vector/VectorStream.h"

#include <memory>

namespace facebook::velox {
namespace {
// An adapter class that can be used to convert a VectorSerializer into a
// BatchVectorSerializer for VectorSerdes that don't want to implement a
// separate serializer.
class DefaultBatchVectorSerializer : public BatchVectorSerializer {
 public:
  DefaultBatchVectorSerializer(
      memory::MemoryPool* pool,
      VectorSerde* serde,
      const VectorSerde::Options* options)
      : pool_(pool), serde_(serde), options_(options) {}

  void serialize(
      const RowVectorPtr& vector,
      const folly::Range<const IndexRange*>& ranges,
      Scratch& scratch,
      OutputStream* stream) override {
    size_t numRows = 0;
    for (const auto& range : ranges) {
      numRows += range.size;
    }

    StreamArena arena(pool_);
    auto serializer = serde_->createIterativeSerializer(
        asRowType(vector->type()), numRows, &arena, options_);
    serializer->append(vector, ranges, scratch);
    serializer->flush(stream);
  }

  void estimateSerializedSize(
      VectorPtr vector,
      const folly::Range<const IndexRange*>& ranges,
      vector_size_t** sizes,
      Scratch& scratch) override {
    serde_->estimateSerializedSize(vector.get(), ranges, sizes, scratch);
  }

 private:
  memory::MemoryPool* const pool_;
  VectorSerde* const serde_;
  const VectorSerde::Options* const options_;
};

std::unique_ptr<VectorSerde>& getVectorSerdeImpl() {
  static std::unique_ptr<VectorSerde> serde;
  return serde;
}

std::unordered_map<VectorSerde::Kind, std::unique_ptr<VectorSerde>>&
getNamedVectorSerdeImpl() {
  static std::unordered_map<VectorSerde::Kind, std::unique_ptr<VectorSerde>>
      namedSerdes;
  return namedSerdes;
}

} // namespace

void IterativeVectorSerializer::append(const RowVectorPtr& vector) {
  const IndexRange allRows{0, vector->size()};
  Scratch scratch;
  append(vector, folly::Range(&allRows, 1), scratch);
}

void BatchVectorSerializer::serialize(
    const RowVectorPtr& vector,
    OutputStream* stream) {
  const IndexRange allRows{0, vector->size()};
  serialize(vector, folly::Range(&allRows, 1), stream);
}

std::unique_ptr<BatchVectorSerializer> VectorSerde::createBatchSerializer(
    memory::MemoryPool* pool,
    const Options* options) {
  return std::make_unique<DefaultBatchVectorSerializer>(pool, this, options);
}

std::string VectorSerde::kindName(Kind kind) {
  switch (kind) {
    case Kind::kPresto:
      return "Presto";
    case Kind::kCompactRow:
      return "CompactRow";
    case Kind::kUnsafeRow:
      return "UnsafeRow";
  }
  VELOX_UNREACHABLE(
      fmt::format("Unknown vector serde kind: {}", static_cast<int32_t>(kind)));
}

VectorSerde::Kind VectorSerde::kindByName(const std::string& kindName) {
  static const std::unordered_map<std::string, Kind> kNameToKind = {
      {"Presto", Kind::kPresto},
      {"CompactRow", Kind::kCompactRow},
      {"UnsafeRow", Kind::kUnsafeRow}};
  const auto it = kNameToKind.find(kindName);
  VELOX_CHECK(
      it != kNameToKind.end(), "Unknown vector serde kind: {}", kindName);
  return it->second;
}

std::ostream& operator<<(std::ostream& out, VectorSerde::Kind kind) {
  out << VectorSerde::kindName(kind);
  return out;
}

VectorSerde* getVectorSerde() {
  auto serde = getVectorSerdeImpl().get();
  VELOX_CHECK_NOT_NULL(serde, "Vector serde is not registered.");
  return serde;
}

/// None of the calls below are thread-safe. We only expect one call of it in
/// the entire system upon startup.
void registerVectorSerde(std::unique_ptr<VectorSerde> serdeToRegister) {
  auto& serde = getVectorSerdeImpl();
  VELOX_CHECK_NULL(serde, "Vector serde is already registered.");
  serde = std::move(serdeToRegister);
}

void deregisterVectorSerde() {
  getVectorSerdeImpl().reset();
}

bool isRegisteredVectorSerde() {
  return getVectorSerdeImpl() != nullptr;
}

/// Named serde helper functions.
void registerNamedVectorSerde(
    VectorSerde::Kind kind,
    std::unique_ptr<VectorSerde> serdeToRegister) {
  auto& namedSerdeMap = getNamedVectorSerdeImpl();
  VELOX_CHECK(
      namedSerdeMap.find(kind) == namedSerdeMap.end(),
      "Vector serde '{}' is already registered.",
      kind);
  namedSerdeMap[kind] = std::move(serdeToRegister);
}

void deregisterNamedVectorSerde(VectorSerde::Kind kind) {
  auto& namedSerdeMap = getNamedVectorSerdeImpl();
  namedSerdeMap.erase(kind);
}

bool isRegisteredNamedVectorSerde(VectorSerde::Kind kind) {
  auto& namedSerdeMap = getNamedVectorSerdeImpl();
  return namedSerdeMap.find(kind) != namedSerdeMap.end();
}

VectorSerde* getNamedVectorSerde(VectorSerde::Kind kind) {
  auto& namedSerdeMap = getNamedVectorSerdeImpl();
  auto it = namedSerdeMap.find(kind);
  VELOX_CHECK(
      it != namedSerdeMap.end(),
      "Named vector serde '{}' is not registered.",
      kind);
  return it->second.get();
}

void VectorStreamGroup::createStreamTree(
    RowTypePtr type,
    int32_t numRows,
    const VectorSerde::Options* options) {
  serializer_ = serde_->createIterativeSerializer(type, numRows, this, options);
}

void VectorStreamGroup::append(
    const RowVectorPtr& vector,
    const folly::Range<const IndexRange*>& ranges,
    Scratch& scratch) {
  serializer_->append(vector, ranges, scratch);
}

void VectorStreamGroup::append(
    const RowVectorPtr& vector,
    const folly::Range<const vector_size_t*>& rows,
    Scratch& scratch) {
  serializer_->append(vector, rows, scratch);
}

void VectorStreamGroup::append(const RowVectorPtr& vector) {
  serializer_->append(vector);
}

void VectorStreamGroup::append(
    const row::CompactRow& compactRow,
    const folly::Range<const vector_size_t*>& rows,
    const std::vector<vector_size_t>& sizes) {
  serializer_->append(compactRow, rows, sizes);
}

void VectorStreamGroup::append(
    const row::UnsafeRowFast& unsafeRow,
    const folly::Range<const vector_size_t*>& rows,
    const std::vector<vector_size_t>& sizes) {
  serializer_->append(unsafeRow, rows, sizes);
}

void VectorStreamGroup::flush(OutputStream* out) {
  serializer_->flush(out);
}

// static
void VectorStreamGroup::estimateSerializedSize(
    const BaseVector* vector,
    const folly::Range<const IndexRange*>& ranges,
    VectorSerde* serde,
    vector_size_t** sizes,
    Scratch& scratch) {
  if (serde == nullptr) {
    getVectorSerde()->estimateSerializedSize(vector, ranges, sizes, scratch);
  } else {
    serde->estimateSerializedSize(vector, ranges, sizes, scratch);
  }
}

// static
void VectorStreamGroup::estimateSerializedSize(
    const BaseVector* vector,
    const folly::Range<const vector_size_t*>& rows,
    VectorSerde* serde,
    vector_size_t** sizes,
    Scratch& scratch) {
  if (serde == nullptr) {
    getVectorSerde()->estimateSerializedSize(vector, rows, sizes, scratch);
  } else {
    serde->estimateSerializedSize(vector, rows, sizes, scratch);
  }
}

// static
void VectorStreamGroup::estimateSerializedSize(
    const row::CompactRow* compactRow,
    const folly::Range<const vector_size_t*>& rows,
    VectorSerde* serde,
    vector_size_t** sizes) {
  if (serde == nullptr) {
    getVectorSerde()->estimateSerializedSize(compactRow, rows, sizes);
  } else {
    serde->estimateSerializedSize(compactRow, rows, sizes);
  }
}

// static
void VectorStreamGroup::estimateSerializedSize(
    const row::UnsafeRowFast* unsafeRow,
    const folly::Range<const vector_size_t*>& rows,
    VectorSerde* serde,
    vector_size_t** sizes) {
  if (serde == nullptr) {
    getVectorSerde()->estimateSerializedSize(unsafeRow, rows, sizes);
  } else {
    serde->estimateSerializedSize(unsafeRow, rows, sizes);
  }
}

// static
void VectorStreamGroup::read(
    ByteInputStream* source,
    velox::memory::MemoryPool* pool,
    RowTypePtr type,
    VectorSerde* serde,
    RowVectorPtr* result,
    const VectorSerde::Options* options) {
  if (serde == nullptr) {
    getVectorSerde()->deserialize(source, pool, type, result, options);
  } else {
    serde->deserialize(source, pool, type, result, options);
  }
}

folly::IOBuf rowVectorToIOBuf(
    const RowVectorPtr& rowVector,
    memory::MemoryPool& pool,
    VectorSerde* serde) {
  return rowVectorToIOBuf(rowVector, rowVector->size(), pool, serde);
}

folly::IOBuf rowVectorToIOBuf(
    const RowVectorPtr& rowVector,
    vector_size_t rangeEnd,
    memory::MemoryPool& pool,
    VectorSerde* serde) {
  auto streamGroup = std::make_unique<VectorStreamGroup>(&pool, serde);
  streamGroup->createStreamTree(asRowType(rowVector->type()), rangeEnd);

  IndexRange range{0, rangeEnd};
  Scratch scratch;
  streamGroup->append(rowVector, folly::Range<IndexRange*>(&range, 1), scratch);

  IOBufOutputStream stream(pool);
  streamGroup->flush(&stream);
  return std::move(*stream.getIOBuf());
}

RowVectorPtr IOBufToRowVector(
    const folly::IOBuf& ioBuf,
    const RowTypePtr& outputType,
    memory::MemoryPool& pool,
    VectorSerde* serde) {
  std::vector<ByteRange> ranges;
  ranges.reserve(4);

  for (const auto& range : ioBuf) {
    ranges.emplace_back(ByteRange{
        const_cast<uint8_t*>(range.data()),
        static_cast<int32_t>(range.size()),
        0});
  }

  auto byteStream = std::make_unique<BufferInputStream>(std::move(ranges));
  RowVectorPtr outputVector;

  // If not supplied, use the default one.
  if (serde == nullptr) {
    serde = getVectorSerde();
  }
  serde->deserialize(
      byteStream.get(), &pool, outputType, &outputVector, nullptr);
  return outputVector;
}

} // namespace facebook::velox
