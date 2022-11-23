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
  if (type.type->kind() == TypeKind::ARRAY) {
    return true;
  }
  if (type.type->kind() == TypeKind::ROW) {
    for (auto i = 0; i < type.getChildren().size(); ++i) {
      if (containsList(type.parquetChildAt(i))) {
        return true;
      }
    }
  }
  return false;
}
} // namespace
using ::parquet::internal::LevelInfo;

LevelMode ParquetTypeWithId::makeLevelInfo(LevelInfo& info) const {
  int16_t repeatedAncestor = 0;
  for (auto parent = parquetParent(); parent;
       parent = parent->parquetParent()) {
    if (parent->type->kind() == TypeKind::ARRAY) {
      repeatedAncestor = parent->maxDefine_;
      break;
    }
  }
  bool isList = type->kind() == TypeKind::ARRAY;
  bool isStruct = type->kind() == TypeKind::ROW;
  bool hasList = false;
  if (isStruct) {
    bool isAllLists = true;
    for (auto i = 0; i < getChildren().size(); ++i) {
      auto child = parquetChildAt(i);
      if (child.type->kind() != TypeKind ::ARRAY) {
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
  if (isStruct) {
    new (&info) LevelInfo(1, maxDefine_, maxRepeat_, repeatedAncestor);
    return hasList ? LevelMode::kStructOverLists : LevelMode::kNulls;
  }
  new (&info) LevelInfo(1, maxDefine_, maxRepeat_, repeatedAncestor);
  return LevelMode::kNulls;
}

} // namespace facebook::velox::parquet
