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

#include "velox/dwio/parquet/reader/ParquetTypeWithId.h"

namespace facebook::velox::parquet {
namespace {
bool containsList(const ParquetTypeWithId& type) {
  if (type.type()->kind() == TypeKind::ARRAY) {
    return true;
  }
  if (type.type()->kind() == TypeKind::ROW) {
    for (auto i = 0; i < type.getChildren().size(); ++i) {
      if (containsList(type.parquetChildAt(i))) {
        return true;
      }
    }
  }
  return false;
}
} // namespace
using arrow::LevelInfo;

std::vector<std::unique_ptr<ParquetTypeWithId::TypeWithId>>
ParquetTypeWithId::moveChildren() && {
  std::vector<std::unique_ptr<TypeWithId>> children;
  for (auto& child : getChildren()) {
    auto type = child->type();
    auto id = child->id();
    auto maxId = child->maxId();
    auto column = child->column();
    auto* parquetChild = (ParquetTypeWithId*)child.get();
    auto name = parquetChild->name_;
    auto parquetType = parquetChild->parquetType_;
    auto logicalType = parquetChild->logicalType_;
    auto maxRepeat = parquetChild->maxRepeat_;
    auto maxDefine = parquetChild->maxDefine_;
    auto isOptional = parquetChild->isOptional_;
    auto isRepeated = parquetChild->isRepeated_;
    auto precision = parquetChild->precision_;
    auto scale = parquetChild->scale_;
    auto typeLength = parquetChild->typeLength_;
    children.push_back(std::make_unique<ParquetTypeWithId>(
        std::move(type),
        std::move(*parquetChild).moveChildren(),
        id,
        maxId,
        column,
        std::move(name),
        parquetType,
        std::move(logicalType),
        maxRepeat,
        maxDefine,
        isOptional,
        isRepeated,
        precision,
        scale,
        typeLength));
  }
  return children;
}

bool ParquetTypeWithId::hasNonRepeatedLeaf() const {
  if (type()->kind() == TypeKind::ARRAY) {
    return false;
  }
  if (type()->kind() == TypeKind::ROW) {
    for (auto i = 0; i < type()->size(); ++i) {
      if (parquetChildAt(i).hasNonRepeatedLeaf()) {
        return true;
      }
    }
    return false;
  } else {
    return true;
  }
}

LevelMode ParquetTypeWithId::makeLevelInfo(LevelInfo& info) const {
  int repeatedAncestor = maxDefine_;
  auto node = this;
  do {
    if (node->isOptional_) {
      repeatedAncestor--;
    }
    node = node->parquetParent();
  } while (node && !node->isRepeated_);
  bool isList = type()->kind() == TypeKind::ARRAY;
  bool isStruct = type()->kind() == TypeKind::ROW;
  bool isMap = type()->kind() == TypeKind::MAP;
  bool hasList = false;
  if (isStruct) {
    bool isAllLists = true;
    for (auto i = 0; i < getChildren().size(); ++i) {
      auto& child = parquetChildAt(i);
      if (child.type()->kind() != TypeKind ::ARRAY) {
        isAllLists = false;
      }
      hasList |= hasList || containsList(child);
    }
  }
  if (isList) {
    // the definition level is the level of a present element.
    new (&info) LevelInfo(1, maxDefine_ + 1, maxRepeat_, repeatedAncestor);
    return LevelMode::kList;
  }

  if (isMap) {
    // the definition level is the level of a present element.
    new (&info) LevelInfo(1, maxDefine_ + 1, maxRepeat_, repeatedAncestor);
    return LevelMode::kList;
  }

  if (isStruct) {
    new (&info) LevelInfo(1, maxDefine_, maxRepeat_, repeatedAncestor);
    return hasList ? LevelMode::kStructOverLists : LevelMode::kNulls;
  }
  new (&info) LevelInfo(1, maxDefine_, maxRepeat_, repeatedAncestor);
  return LevelMode::kNulls;
}

} // namespace facebook::velox::parquet
