/*
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
#include "presto_cpp/main/types/TupleDomainBuilder.h"

#include <boost/algorithm/string.hpp>
#include <sstream>
#include "velox/common/encode/Base64.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/vector/FlatVector.h"

namespace facebook::presto {

namespace {

/// Creates a VectorPtr containing a single value of the given type.
velox::VectorPtr createSingleValueVector(
    const velox::TypePtr& type,
    const velox::variant& value,
    velox::memory::MemoryPool* pool) {
  return velox::BaseVector::createConstant(type, value, 1, pool);
}

/// Serializes a VectorPtr to a Base64-encoded string using PrestoVectorSerde.
std::string serializeVectorToBase64(
    const velox::VectorPtr& vector,
    velox::memory::MemoryPool* pool) {
  auto serde =
      std::make_unique<velox::serializer::presto::PrestoVectorSerde>();
  std::ostringstream output;
  serde->serializeSingleColumn(vector, nullptr, pool, &output);
  const auto serialized = output.str();
  return velox::encoding::Base64::encode(
      serialized.c_str(), serialized.size());
}

/// Builds a protocol::Marker with the given type, value, and bound.
protocol::Marker buildMarker(
    const std::string& prestoType,
    const velox::TypePtr& veloxType,
    const velox::variant& value,
    protocol::Bound bound,
    velox::memory::MemoryPool* pool) {
  protocol::Marker marker;
  marker.type = prestoType;
  marker.bound = bound;

  auto vector = createSingleValueVector(veloxType, value, pool);
  auto block = std::make_shared<protocol::Block>();
  block->data = serializeVectorToBase64(vector, pool);
  marker.valueBlock = block;

  return marker;
}

/// Builds a Range where low and high are both EXACTLY(value).
protocol::Range buildExactRange(
    const std::string& prestoType,
    const velox::TypePtr& veloxType,
    const velox::variant& value,
    velox::memory::MemoryPool* pool) {
  protocol::Range range;
  range.low =
      buildMarker(prestoType, veloxType, value, protocol::Bound::EXACTLY, pool);
  range.high =
      buildMarker(prestoType, veloxType, value, protocol::Bound::EXACTLY, pool);
  return range;
}

} // namespace

std::string toPrestoTypeName(const velox::TypePtr& type) {
  std::string signature = type->toString();
  if (type->isPrimitiveType()) {
    boost::algorithm::to_lower(signature);
  } else {
    boost::algorithm::erase_all(signature, "\"\"");
    boost::algorithm::replace_all(signature, ":", " ");
    boost::algorithm::replace_all(signature, "<", "(");
    boost::algorithm::replace_all(signature, ">", ")");
    boost::algorithm::to_lower(signature);
  }
  return signature;
}

std::string serializeValueToBlock(
    const velox::TypePtr& type,
    const velox::variant& value,
    velox::memory::MemoryPool* pool) {
  auto vector = createSingleValueVector(type, value, pool);
  return serializeVectorToBase64(vector, pool);
}

protocol::TupleDomain<std::string> buildTupleDomain(
    const std::string& columnName,
    const velox::TypePtr& type,
    const std::vector<velox::variant>& discreteValues,
    const std::optional<velox::variant>& minValue,
    const std::optional<velox::variant>& maxValue,
    bool nullAllowed,
    velox::memory::MemoryPool* pool) {
  auto prestoType = toPrestoTypeName(type);

  // Build ranges.
  std::vector<protocol::Range> ranges;
  if (!discreteValues.empty()) {
    ranges.reserve(discreteValues.size());
    for (const auto& value : discreteValues) {
      ranges.push_back(buildExactRange(prestoType, type, value, pool));
    }
  } else if (minValue.has_value() && maxValue.has_value()) {
    protocol::Range range;
    range.low = buildMarker(
        prestoType, type, *minValue, protocol::Bound::EXACTLY, pool);
    range.high = buildMarker(
        prestoType, type, *maxValue, protocol::Bound::EXACTLY, pool);
    ranges.push_back(std::move(range));
  }

  // Build SortedRangeSet.
  auto sortedRangeSet = std::make_shared<protocol::SortedRangeSet>();
  sortedRangeSet->type = prestoType;
  sortedRangeSet->ranges = std::move(ranges);

  // Build Domain.
  protocol::Domain domain;
  domain.values = sortedRangeSet;
  domain.nullAllowed = nullAllowed;

  // Build TupleDomain with single column.
  protocol::TupleDomain<std::string> tupleDomain;
  tupleDomain.domains =
      std::make_shared<std::map<std::string, protocol::Domain>>();
  tupleDomain.domains->emplace(columnName, std::move(domain));

  return tupleDomain;
}

protocol::TupleDomain<std::string> buildNoneTupleDomain() {
  protocol::TupleDomain<std::string> tupleDomain;
  // nullptr domains represents none() (empty build side).
  tupleDomain.domains = nullptr;
  return tupleDomain;
}

} // namespace facebook::presto
