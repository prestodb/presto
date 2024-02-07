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
#include "velox/common/base/RawVector.h"

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

 private:
  memory::MemoryPool* const pool_;
  VectorSerde* const serde_;
  const VectorSerde::Options* const options_;
};

std::unique_ptr<VectorSerde>& getVectorSerdeImpl() {
  static std::unique_ptr<VectorSerde> serde;
  return serde;
}

std::unordered_map<std::string, std::unique_ptr<VectorSerde>>&
getNamedVectorSerdeImpl() {
  static std::unordered_map<std::string, std::unique_ptr<VectorSerde>>
      namedSerde;
  return namedSerde;
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
    std::string_view serdeName,
    std::unique_ptr<VectorSerde> serdeToRegister) {
  auto& namedSerdeMap = getNamedVectorSerdeImpl();
  VELOX_CHECK(
      namedSerdeMap.find(std::string(serdeName)) == namedSerdeMap.end(),
      "Vector serde '{}' is already registered.",
      serdeName);
  namedSerdeMap[std::string(serdeName)] = std::move(serdeToRegister);
}

void deregisterNamedVectorSerde(std::string_view serdeName) {
  auto& namedSerdeMap = getNamedVectorSerdeImpl();
  namedSerdeMap.erase(std::string(serdeName));
}

bool isRegisteredNamedVectorSerde(std::string_view serdeName) {
  auto& namedSerdeMap = getNamedVectorSerdeImpl();
  return namedSerdeMap.find(std::string(serdeName)) != namedSerdeMap.end();
}

VectorSerde* getNamedVectorSerde(std::string_view serdeName) {
  auto& namedSerdeMap = getNamedVectorSerdeImpl();
  auto it = namedSerdeMap.find(std::string(serdeName));
  VELOX_CHECK(
      it != namedSerdeMap.end(),
      "Named vector serde '{}' is not registered.",
      serdeName);
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

void VectorStreamGroup::flush(OutputStream* out) {
  serializer_->flush(out);
}

// static
void VectorStreamGroup::estimateSerializedSize(
    VectorPtr vector,
    const folly::Range<const IndexRange*>& ranges,
    vector_size_t** sizes,
    Scratch& scratch) {
  getVectorSerde()->estimateSerializedSize(vector, ranges, sizes, scratch);
}

// static
void VectorStreamGroup::estimateSerializedSize(
    VectorPtr vector,
    folly::Range<const vector_size_t*> rows,
    vector_size_t** sizes,
    Scratch& scratch) {
  getVectorSerde()->estimateSerializedSize(vector, rows, sizes, scratch);
}

// static
void VectorStreamGroup::read(
    ByteInputStream* source,
    velox::memory::MemoryPool* pool,
    RowTypePtr type,
    RowVectorPtr* result,
    const VectorSerde::Options* options) {
  getVectorSerde()->deserialize(source, pool, type, result, options);
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
        const_cast<uint8_t*>(range.data()), (int32_t)range.size(), 0});
  }

  ByteInputStream byteStream(std::move(ranges));
  RowVectorPtr outputVector;

  // If not supplied, use the default one.
  if (serde == nullptr) {
    serde = getVectorSerde();
  }
  serde->deserialize(&byteStream, &pool, outputType, &outputVector, nullptr);
  return outputVector;
}

} // namespace facebook::velox
