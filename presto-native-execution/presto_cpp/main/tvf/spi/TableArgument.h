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

#include "presto_cpp/main/tvf/spi/Argument.h"

#include "velox/core/Expressions.h"
#include "velox/core/PlanNode.h"

namespace facebook::presto::tvf {

class TableArgument : public Argument {
 public:
  TableArgument(velox::RowTypePtr type,
                std::vector<velox::core::FieldAccessTypedExprPtr> partitionKeys = {},
                std::vector<velox::core::FieldAccessTypedExprPtr> sortingKeys = {},
                std::vector<velox::core::SortOrder> sortingOrders = {})
      : rowType_(std::move(type)),
        partitionKeys_(std::move(partitionKeys)),
        sortingKeys_(std::move(sortingKeys)),
        sortingOrders_(std::move(sortingOrders)) {
    VELOX_CHECK_EQ(
        sortingKeys_.size(),
        sortingOrders_.size(),
        "Number of sorting keys must be equal to the number of sorting orders");

    std::unordered_set<std::string> keyNames;
    for (const auto& key : partitionKeys_) {
      VELOX_USER_CHECK(
          keyNames.insert(key->name()).second,
          "Partitioning keys must be unique. Found duplicate key: {}",
          key->name());
    }

    for (const auto& key : sortingKeys_) {
      VELOX_USER_CHECK(
          keyNames.insert(key->name()).second,
          "Sorting keys must be unique and not overlap with partitioning keys. Found duplicate key: {}",
          key->name());
    }
  }

  velox::RowTypePtr rowType() const {
    return rowType_;
  }

  const std::vector<velox::core::FieldAccessTypedExprPtr>& partitionKeys() const {
    return partitionKeys_;
  }

  const std::vector<velox::core::FieldAccessTypedExprPtr>& sortingKeys() const {
    return sortingKeys_;
  }

  const std::vector<velox::core::SortOrder>& sortingOrders() const {
    return sortingOrders_;
  }

 private:
  const velox::RowTypePtr rowType_;
  const std::vector<velox::core::FieldAccessTypedExprPtr> partitionKeys_;
  const std::vector<velox::core::FieldAccessTypedExprPtr> sortingKeys_;
  const std::vector<velox::core::SortOrder> sortingOrders_;
};

class TableArgumentSpecification : public ArgumentSpecification {
 public:
  TableArgumentSpecification(std::string name, bool rowSemantics, bool pruneWhenEmpty, bool passThroughColumns)
      : ArgumentSpecification(name, true),
        rowSemantics_(rowSemantics),
        pruneWhenEmpty_(pruneWhenEmpty),
        passThroughColumns_(passThroughColumns) {};

  bool rowSemantics() const {
    return rowSemantics_;
  }

  bool pruneWhenEmpty() const {
    return pruneWhenEmpty_;
  }

  bool passThroughColumns() const {
    return passThroughColumns_;
  }

 private:
  const bool rowSemantics_;
  const bool pruneWhenEmpty_;
  const bool passThroughColumns_;
};

using TableArgumentSpecList =
    std::unordered_set<std::shared_ptr<ArgumentSpecification>>;

} // namespace facebook::presto::tvf
