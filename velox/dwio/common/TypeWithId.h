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
#include "velox/type/Type.h"

namespace facebook::velox::dwio::common {

class TypeWithId : public velox::Tree<std::shared_ptr<const TypeWithId>> {
 public:
  TypeWithId(
      std::shared_ptr<const velox::Type> type,
      std::vector<std::shared_ptr<const TypeWithId>>&& children,
      uint32_t id,
      uint32_t maxId,
      uint32_t column);

  static std::shared_ptr<const TypeWithId> create(
      const std::shared_ptr<const velox::Type>& root,
      uint32_t next = 0);

  uint32_t size() const override;

  const std::shared_ptr<const TypeWithId>& childAt(uint32_t idx) const override;

  const std::shared_ptr<const TypeWithId>& childByName(
      const std::string& name) const {
    VELOX_CHECK_EQ(type->kind(), velox::TypeKind::ROW);
    return childAt(type->as<velox::TypeKind::ROW>().getChildIdx(name));
  }

  const std::vector<std::shared_ptr<const TypeWithId>> getChildren() const {
    return children_;
  }

  const std::shared_ptr<const velox::Type> type;
  const TypeWithId* const parent;
  const uint32_t id;
  const uint32_t maxId;
  const uint32_t column;

 private:
  const std::vector<std::shared_ptr<const TypeWithId>> children_;

  static std::shared_ptr<const TypeWithId> create_(
      const std::shared_ptr<const velox::Type>& type,
      uint32_t& next,
      uint32_t column);
};

} // namespace facebook::velox::dwio::common
