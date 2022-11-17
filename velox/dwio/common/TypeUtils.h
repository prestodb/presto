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
#include "velox/dwio/common/exception/Exception.h"

#include <unordered_set>

namespace facebook::velox::dwio::common::typeutils {
class SelectedTypeBuilder {
 public:
  static std::shared_ptr<const TypeWithId> build(
      const std::shared_ptr<const TypeWithId>& typeWithId,
      const std::function<bool(size_t)>& selector) {
    return visit(typeWithId, selector);
  }

 private:
  static void checkChildrenSelected(
      const std::shared_ptr<const TypeWithId>& type,
      const std::function<bool(size_t)>& selector) {
    for (size_t i = 0; i < type->size(); ++i) {
      auto& child = type->childAt(i);
      if (!selector(child->id)) {
        std::string s{};
        s += "invalid type selection: parent ";
        s += type->type->toString();
        s += " is selected, but child (index: ";
        s += i;
        s += ", id: ";
        s += std::to_string(type->id);
        s += ") is not";
        throw std::invalid_argument{s};
      }
    }
  }

  static std::shared_ptr<const TypeWithId> visit(
      const std::shared_ptr<const TypeWithId>& typeWithId,
      const std::function<bool(size_t)>& selector) {
    if (typeWithId->type->isPrimitiveType()) {
      return typeWithId;
    }
    if (typeWithId->type->isRow()) {
      std::vector<std::string> names;
      std::vector<std::shared_ptr<const TypeWithId>> typesWithIds;
      std::vector<std::shared_ptr<const velox::Type>> types;
      auto& row = typeWithId->type->asRow();
      for (auto i = 0; i < typeWithId->size(); ++i) {
        auto& child = typeWithId->childAt(i);
        if (selector(child->id)) {
          names.push_back(row.nameOf(i));
          std::shared_ptr<const TypeWithId> twid;
          twid = visit(child, selector);
          typesWithIds.push_back(twid);
          types.push_back(twid->type);
        }
      }
      if (types.empty()) {
        throw std::invalid_argument{
            "selected nothing from row: " + row.toString()};
      }
      return std::make_shared<TypeWithId>(
          velox::ROW(std::move(names), std::move(types)),
          std::move(typesWithIds),
          typeWithId->id,
          typeWithId->maxId,
          typeWithId->column);
    } else {
      checkChildrenSelected(typeWithId, selector);
      std::vector<std::shared_ptr<const TypeWithId>> typesWithIds;
      std::vector<std::shared_ptr<const velox::Type>> types;
      for (auto i = 0; i < typeWithId->size(); ++i) {
        auto& child = typeWithId->childAt(i);
        std::shared_ptr<const TypeWithId> twid = visit(child, selector);
        typesWithIds.push_back(twid);
        types.push_back(twid->type);
      }
      auto type = velox::createType(typeWithId->type->kind(), std::move(types));
      return std::make_shared<TypeWithId>(
          type,
          std::move(typesWithIds),
          typeWithId->id,
          typeWithId->maxId,
          typeWithId->column);
    }
  }
};

class CompatChecker {
 public:
  static void check(
      const velox::Type& from,
      const velox::Type& to,
      bool recurse = false,
      const std::function<std::string()>& exceptionMessageCreator = []() {
        return "";
      }) {
    static CompatChecker checker;
    checker.checkImpl(from, to, recurse, exceptionMessageCreator);
  }

 private:
  void checkImpl(
      const velox::Type& from,
      const velox::Type& to,
      bool recurse,
      const std::function<std::string()>& exceptionMessageCreator) const {
    if (!compat(from.kind(), to.kind())) {
      VELOX_SCHEMA_MISMATCH_ERROR(fmt::format(
          "{}, From Kind: {}, To Kind: {}",
          exceptionMessageCreator ? exceptionMessageCreator() : "",
          velox::mapTypeKindToName(from.kind()),
          velox::mapTypeKindToName(to.kind())));
    }

    if (recurse) {
      switch (from.kind()) {
        case velox::TypeKind::ROW: {
          uint64_t childCount = std::min(from.size(), to.size());
          for (uint64_t i = 0; i < childCount; ++i) {
            checkImpl(
                *from.childAt(i),
                *to.childAt(i),
                true,
                exceptionMessageCreator);
          }
          break;
        }
        case velox::TypeKind::ARRAY:
          checkImpl(
              *from.childAt(0), *to.childAt(0), true, exceptionMessageCreator);
          break;
        case velox::TypeKind::MAP: {
          checkImpl(
              *from.childAt(0), *to.childAt(0), true, exceptionMessageCreator);
          checkImpl(
              *from.childAt(1), *to.childAt(1), true, exceptionMessageCreator);
          break;
        }
        default:
          break;
      }
    }
  }

  static int32_t getKey(velox::TypeKind from, velox::TypeKind to) {
    auto fromVal = static_cast<uint32_t>(from);
    auto toVal = static_cast<uint32_t>(to);
    auto limit = std::numeric_limits<uint16_t>::max();
    DWIO_ENSURE(fromVal < limit && toVal < limit, "invalid range of type kind");
    return (fromVal << 16) | toVal;
  }

  bool compat(velox::TypeKind from, velox::TypeKind to) const {
    return from == to || compat_.find(getKey(from, to)) != compat_.end();
  }

  void setCompat(velox::TypeKind from, velox::TypeKind to) {
    compat_.insert(getKey(from, to));
  }

  CompatChecker() {
    setCompat(velox::TypeKind::BOOLEAN, velox::TypeKind::TINYINT);
    setCompat(velox::TypeKind::BOOLEAN, velox::TypeKind::SMALLINT);
    setCompat(velox::TypeKind::BOOLEAN, velox::TypeKind::INTEGER);
    setCompat(velox::TypeKind::BOOLEAN, velox::TypeKind::BIGINT);
    setCompat(velox::TypeKind::TINYINT, velox::TypeKind::SMALLINT);
    setCompat(velox::TypeKind::TINYINT, velox::TypeKind::INTEGER);
    setCompat(velox::TypeKind::TINYINT, velox::TypeKind::BIGINT);
    setCompat(velox::TypeKind::SMALLINT, velox::TypeKind::INTEGER);
    setCompat(velox::TypeKind::SMALLINT, velox::TypeKind::BIGINT);
    setCompat(velox::TypeKind::INTEGER, velox::TypeKind::BIGINT);
    setCompat(velox::TypeKind::REAL, velox::TypeKind::DOUBLE);
  }

  ~CompatChecker() = default;

  std::unordered_set<uint32_t> compat_;
};

} // namespace facebook::velox::dwio::common::typeutils
