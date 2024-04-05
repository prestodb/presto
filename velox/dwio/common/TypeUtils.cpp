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

#include "velox/dwio/common/TypeUtils.h"
#include "velox/dwio/common/exception/Exception.h"

#include <unordered_set>

namespace facebook::velox::dwio::common::typeutils {
namespace {

void checkChildrenSelected(
    const std::shared_ptr<const TypeWithId>& type,
    const std::function<bool(size_t)>& selector) {
  for (size_t i = 0; i < type->size(); ++i) {
    VELOX_USER_CHECK(
        selector(type->childAt(i)->id()),
        folly::to<std::string>(
            "invalid type selection: parent ",
            type->type()->toString(),
            " is selected, but child (index: ",
            i,
            ", id: ",
            std::to_string(type->id()),
            ") is not"));
  }
}

std::unique_ptr<TypeWithId> visit(
    const std::shared_ptr<const TypeWithId>& typeWithId,
    const std::function<bool(size_t)>& selector) {
  if (typeWithId->type()->isPrimitiveType()) {
    return std::make_unique<TypeWithId>(
        typeWithId->type(),
        std::vector<std::unique_ptr<TypeWithId>>(),
        typeWithId->id(),
        typeWithId->maxId(),
        typeWithId->column());
  }
  if (typeWithId->type()->isRow()) {
    std::vector<std::string> names;
    std::vector<std::unique_ptr<TypeWithId>> selectedChildren;
    std::vector<std::shared_ptr<const Type>> types;
    auto& row = typeWithId->type()->asRow();
    for (auto i = 0; i < typeWithId->size(); ++i) {
      auto& child = typeWithId->childAt(i);
      if (selector(child->id())) {
        names.push_back(row.nameOf(i));
        auto newChild = visit(child, selector);
        types.push_back(newChild->type());
        selectedChildren.push_back(std::move(newChild));
      }
    }
    VELOX_USER_CHECK(
        !types.empty(), "selected nothing from row: " + row.toString());
    return std::make_unique<TypeWithId>(
        ROW(std::move(names), std::move(types)),
        std::move(selectedChildren),
        typeWithId->id(),
        typeWithId->maxId(),
        typeWithId->column());
  } else {
    checkChildrenSelected(typeWithId, selector);
    std::vector<std::unique_ptr<TypeWithId>> selectedChildren;
    std::vector<std::shared_ptr<const Type>> types;
    for (auto i = 0; i < typeWithId->size(); ++i) {
      auto& child = typeWithId->childAt(i);
      auto newChild = visit(child, selector);
      types.push_back(newChild->type());
      selectedChildren.push_back(std::move(newChild));
    }
    auto type = createType(typeWithId->type()->kind(), std::move(types));
    return std::make_unique<TypeWithId>(
        type,
        std::move(selectedChildren),
        typeWithId->id(),
        typeWithId->maxId(),
        typeWithId->column());
  }
}

uint32_t getKey(TypeKind from, TypeKind to) {
  auto fromVal = static_cast<uint32_t>(from);
  auto toVal = static_cast<uint32_t>(to);
  auto limit = std::numeric_limits<uint16_t>::max();
  DWIO_ENSURE(fromVal < limit && toVal < limit, "invalid range of type kind");
  return (fromVal << 16) | toVal;
}

std::unordered_set<uint32_t> makeCompatibilityMap() {
  std::unordered_set<uint32_t> compat;
  compat.insert(getKey(TypeKind::BOOLEAN, TypeKind::TINYINT));
  compat.insert(getKey(TypeKind::BOOLEAN, TypeKind::SMALLINT));
  compat.insert(getKey(TypeKind::BOOLEAN, TypeKind::INTEGER));
  compat.insert(getKey(TypeKind::BOOLEAN, TypeKind::BIGINT));
  compat.insert(getKey(TypeKind::TINYINT, TypeKind::SMALLINT));
  compat.insert(getKey(TypeKind::TINYINT, TypeKind::INTEGER));
  compat.insert(getKey(TypeKind::TINYINT, TypeKind::BIGINT));
  compat.insert(getKey(TypeKind::SMALLINT, TypeKind::INTEGER));
  compat.insert(getKey(TypeKind::SMALLINT, TypeKind::BIGINT));
  compat.insert(getKey(TypeKind::INTEGER, TypeKind::BIGINT));
  compat.insert(getKey(TypeKind::BIGINT, TypeKind::HUGEINT));
  compat.insert(getKey(TypeKind::REAL, TypeKind::DOUBLE));
  return compat;
}

bool isCompatible(TypeKind from, TypeKind to) {
  static auto compat = makeCompatibilityMap();
  return from == to || compat.find(getKey(from, to)) != compat.end();
}

template <typename T, typename FKind, typename FShouldRead>
void checkTypeCompatibility(
    const Type& from,
    const T& to,
    bool recurse,
    const FKind& kind,
    const FShouldRead& shouldRead,
    const std::function<std::string()>& exceptionMessageCreator) {
  if (shouldRead(to) && !isCompatible(from.kind(), kind(to))) {
    VELOX_SCHEMA_MISMATCH_ERROR(fmt::format(
        "{}, From Kind: {}, To Kind: {}",
        exceptionMessageCreator ? exceptionMessageCreator() : "Schema mismatch",
        mapTypeKindToName(from.kind()),
        mapTypeKindToName(kind(to))));
  }

  if (recurse) {
    uint64_t childCount = std::min(from.size(), to.size());
    for (uint64_t i = 0; i < childCount; ++i) {
      checkTypeCompatibility(
          *from.childAt(i),
          *to.childAt(i),
          true,
          kind,
          shouldRead,
          exceptionMessageCreator);
    }
  }
}

} // namespace

std::shared_ptr<const TypeWithId> buildSelectedType(
    const std::shared_ptr<const TypeWithId>& typeWithId,
    const std::function<bool(size_t)>& selector) {
  return visit(typeWithId, selector);
}

void checkTypeCompatibility(
    const Type& from,
    const Type& to,
    bool recurse,
    const std::function<std::string()>& exceptionMessageCreator) {
  return checkTypeCompatibility(
      from,
      to,
      recurse,
      [](const auto& t) { return t.kind(); },
      [](const auto& /* ignored */) { return true; },
      exceptionMessageCreator);
}

void checkTypeCompatibility(
    const Type& from,
    const ColumnSelector& selector,
    const std::function<std::string()>& exceptionMessageCreator) {
  return checkTypeCompatibility(
      from,
      *selector.getSchemaWithId(),
      /*recurse=*/true,
      [](const auto& t) { return t.type()->kind(); },
      [&selector](const auto& node) {
        return selector.shouldReadNode(node.id());
      },
      exceptionMessageCreator);
}

} // namespace facebook::velox::dwio::common::typeutils
