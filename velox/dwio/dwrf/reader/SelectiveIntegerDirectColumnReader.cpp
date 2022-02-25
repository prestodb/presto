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

#include "velox/dwio/dwrf/reader/SelectiveIntegerDirectColumnReader.h"

namespace facebook::velox::dwrf {

uint64_t SelectiveIntegerDirectColumnReader::skip(uint64_t numValues) {
  numValues = ColumnReader::skip(numValues);
  ints->skip(numValues);
  return numValues;
}

void SelectiveIntegerDirectColumnReader::read(
    vector_size_t offset,
    RowSet rows,
    const uint64_t* incomingNulls) {
  VELOX_WIDTH_DISPATCH(
      sizeOfIntKind(type_->kind()), prepareRead, offset, rows, incomingNulls);
  bool isDense = rows.back() == rows.size() - 1;
  common::Filter* filter =
      scanSpec_->filter() ? scanSpec_->filter() : &alwaysTrue();
  if (scanSpec_->keepValues()) {
    if (scanSpec_->valueHook()) {
      if (isDense) {
        processValueHook<true>(rows, scanSpec_->valueHook());
      } else {
        processValueHook<false>(rows, scanSpec_->valueHook());
      }
      return;
    }
    if (isDense) {
      processFilter<true>(filter, ExtractToReader(this), rows);
    } else {
      processFilter<false>(filter, ExtractToReader(this), rows);
    }
  } else {
    if (isDense) {
      processFilter<true>(filter, DropValues(), rows);
    } else {
      processFilter<false>(filter, DropValues(), rows);
    }
  }
}

} // namespace facebook::velox::dwrf
