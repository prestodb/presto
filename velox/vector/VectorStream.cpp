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
  serializer_ =
      getVectorSerde()->createSerializer(type, numRows, this, options);
}

void VectorStreamGroup::append(
    RowVectorPtr vector,
    const folly::Range<const IndexRange*>& ranges) {
  serializer_->append(vector, ranges);
}

void VectorStreamGroup::flush(OutputStream* out) {
  serializer_->flush(out);
}

// static
void VectorStreamGroup::estimateSerializedSize(
    VectorPtr vector,
    const folly::Range<const IndexRange*>& ranges,
    vector_size_t** sizes) {
  getVectorSerde()->estimateSerializedSize(vector, ranges, sizes);
}

// static
void VectorStreamGroup::read(
    ByteStream* source,
    velox::memory::MemoryPool* pool,
    RowTypePtr type,
    RowVectorPtr* result,
    const VectorSerde::Options* options) {
  getVectorSerde()->deserialize(source, pool, type, result, options);
}

} // namespace facebook::velox
