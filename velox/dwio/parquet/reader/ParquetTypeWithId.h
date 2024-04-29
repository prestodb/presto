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

#include "velox/dwio/common/TypeWithId.h"
#include "velox/dwio/parquet/thrift/ParquetThriftTypes.h"
#include "velox/dwio/parquet/writer/arrow/LevelConversion.h"

namespace facebook::velox::parquet {

/// Describes what to extract from leaf repetition / definition
/// levels for a particular level. Selects between using
/// DefLevelsToBitmap, DefRepLevelsToList or DefRepLevelsToBitmap
/// respectively.
enum class LevelMode { kList, kNulls, kStructOverLists };

// Describes a Parquet column.
class ParquetTypeWithId : public dwio::common::TypeWithId {
 public:
  // Occurs in 'column' for non-leaf nodes.
  static constexpr uint32_t kNonLeaf = ~0;

  ParquetTypeWithId(
      TypePtr type,
      std::vector<std::unique_ptr<TypeWithId>>&& children,
      uint32_t id,
      uint32_t maxId,
      uint32_t column,
      std::string name,
      std::optional<thrift::Type::type> parquetType,
      std::optional<thrift::LogicalType> logicalType,
      uint32_t maxRepeat,
      uint32_t maxDefine,
      bool isOptional,
      bool isRepeated,
      int32_t precision = 0,
      int32_t scale = 0,
      int32_t typeLength = 0)
      : TypeWithId(type, std::move(children), id, maxId, column),
        name_(name),
        parquetType_(parquetType),
        logicalType_(std::move(logicalType)),
        maxRepeat_(maxRepeat),
        maxDefine_(maxDefine),
        isOptional_(isOptional),
        isRepeated_(isRepeated),
        precision_(precision),
        scale_(scale),
        typeLength_(typeLength) {}

  bool isLeaf() const {
    // Negative column ordinal means non-leaf column.
    return static_cast<int32_t>(column()) >= 0;
  }

  const ParquetTypeWithId& parquetChildAt(uint32_t index) const {
    return *reinterpret_cast<const ParquetTypeWithId*>(childAt(index).get());
  }

  const ParquetTypeWithId* parquetParent() const {
    return reinterpret_cast<const ParquetTypeWithId*>(parent());
  }

  /// Fills 'info' and returns the mode for interpreting levels.
  LevelMode makeLevelInfo(arrow::LevelInfo& info) const;

  std::vector<std::unique_ptr<ParquetTypeWithId::TypeWithId>> moveChildren() &&;

  const std::string name_;
  const std::optional<thrift::Type::type> parquetType_;
  const std::optional<thrift::LogicalType> logicalType_;
  const uint32_t maxRepeat_;
  const uint32_t maxDefine_;
  const bool isOptional_;
  const bool isRepeated_;
  const int32_t precision_;
  const int32_t scale_;
  const int32_t typeLength_;

  // True if this is or has a non-repeated leaf.
  bool hasNonRepeatedLeaf() const;
};

using ParquetTypeWithIdPtr = std::shared_ptr<const ParquetTypeWithId>;
} // namespace facebook::velox::parquet
