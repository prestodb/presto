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

#include <memory>
#include <vector>
#include "velox/dwio/common/ScanSpec.h"
#include "velox/type/Type.h"

namespace facebook::velox::dwio::common {

class TypeWithId : public velox::Tree<std::shared_ptr<const TypeWithId>> {
 public:
  /// NOTE: This constructor will re-parent the children.
  TypeWithId(
      std::shared_ptr<const velox::Type> type,
      std::vector<std::unique_ptr<TypeWithId>>&& children,
      uint32_t id,
      uint32_t maxId,
      uint32_t column);

  TypeWithId(const TypeWithId&) = delete;
  TypeWithId& operator=(const TypeWithId&) = delete;

  static std::unique_ptr<TypeWithId> create(
      const std::shared_ptr<const velox::Type>& root,
      uint32_t next = 0);

  /// Create TypeWithId node but leave all the unselected children as nullptr.
  /// The ids are set correctly even when some of the previous nodes are not
  /// selected.
  static std::unique_ptr<TypeWithId> create(
      const RowTypePtr& type,
      const velox::common::ScanSpec& spec);

  uint32_t size() const override;

  const std::shared_ptr<const velox::Type>& type() const {
    return type_;
  }

  const TypeWithId* parent() const {
    return parent_;
  }

  uint32_t id() const {
    return id_;
  }

  uint32_t maxId() const {
    return maxId_;
  }

  uint32_t column() const {
    return column_;
  }

  const std::shared_ptr<const TypeWithId>& childAt(uint32_t idx) const override;

  const std::shared_ptr<const TypeWithId>& childByName(
      const std::string& name) const {
    VELOX_CHECK_EQ(type_->kind(), velox::TypeKind::ROW);
    return childAt(type_->as<velox::TypeKind::ROW>().getChildIdx(name));
  }

  const std::vector<std::shared_ptr<const TypeWithId>>& getChildren() const {
    return children_;
  }

  std::string fullName() const;

 private:
  static std::unique_ptr<TypeWithId> create(
      const std::shared_ptr<const velox::Type>& type,
      uint32_t& next,
      uint32_t column);

  const std::shared_ptr<const velox::Type> type_;
  const TypeWithId* const parent_;
  const uint32_t id_;
  const uint32_t maxId_;
  const uint32_t column_;
  const std::vector<std::shared_ptr<const TypeWithId>> children_;
};

} // namespace facebook::velox::dwio::common
