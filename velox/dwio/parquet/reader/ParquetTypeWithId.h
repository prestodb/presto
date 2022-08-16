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

namespace facebook::velox::parquet {

// Describes a Parquet column.
class ParquetTypeWithId : public dwio::common::TypeWithId {
 public:
  // Occurs in 'column' for non-leaf nodes.
  static constexpr uint32_t kNonLeaf = ~0;

  ParquetTypeWithId(
      TypePtr type,
      std::vector<std::shared_ptr<const TypeWithId>>&& children,
      uint32_t id,
      uint32_t maxId,
      uint32_t column,
      std::string name,
      std::optional<thrift::Type::type> parquetType,
      uint32_t maxRepeat,
      uint32_t maxDefine,
      int32_t precision = 0,
      int32_t scale = 0,
      int32_t typeLength = 0)
      : TypeWithId(type, std::move(children), id, maxId, column),
        name_(name),
        parquetType_(parquetType),
        maxRepeat_(maxRepeat),
        maxDefine_(maxDefine),
        precision_(precision),
        scale_(scale),
        typeLength_(typeLength) {}

  bool isLeaf() const {
    // Negative column ordinal means non-leaf column.
    return static_cast<int32_t>(column) >= 0;
  }

  const ParquetTypeWithId& parquetChildAt(uint32_t index) const {
    return *reinterpret_cast<const ParquetTypeWithId*>(childAt(index).get());
  }

  const std::string name_;
  const std::optional<thrift::Type::type> parquetType_;
  const uint32_t maxRepeat_;
  const uint32_t maxDefine_;
  const int32_t precision_;
  const int32_t scale_;
  const int32_t typeLength_;
};

using ParquetTypeWithIdPtr = std::shared_ptr<const ParquetTypeWithId>;
} // namespace facebook::velox::parquet
