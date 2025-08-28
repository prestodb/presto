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

#include "velox/tpcds/gen/utils/append_info-c.h"

#include <fmt/format.h>
#include "velox/tpcds/gen/dsdgen/include/append_info.h"
#include "velox/tpcds/gen/dsdgen/include/config.h"
#include "velox/tpcds/gen/dsdgen/include/date.h"
#include "velox/tpcds/gen/dsdgen/include/nulls.h"
#include "velox/tpcds/gen/dsdgen/include/porting.h"
#include "velox/vector/FlatVector.h"

using namespace facebook::velox;

append_info* append_info_get(void* info_list, int table_id) {
  auto& append_vector =
      *((std::vector<std::unique_ptr<tpcds::TpcdsTableDef>>*)info_list);
  return (append_info*)append_vector[table_id].get();
}

bool facebook::velox::tpcds::TpcdsTableDef::IsNull(int32_t column) {
  return nullCheck(column, *dsdGenContext);
}

void append_row_start(append_info /*info*/) {}

void append_row_end(append_info info) {
  auto append_info = (tpcds::TpcdsTableDef*)info;
  append_info->colIndex %= append_info->children.size();
  append_info->rowIndex++;
}

void append_varchar(
    int32_t column,
    append_info info,
    const char* value,
    bool fillEmptyStringAsNull) {
  auto append_info = (tpcds::TpcdsTableDef*)info;
  if (((append_info->IsNull(column)) || (!value) || (*value == '\0')) &&
      (fillEmptyStringAsNull)) {
    append_info->children[append_info->colIndex]->setNull(
        append_info->rowIndex, true);
  } else {
    append_info->children[append_info->colIndex]
        ->asFlatVector<StringView>()
        ->set(append_info->rowIndex, value);
  }
  append_info->colIndex++;
}

void append_varchar(
    int32_t column,
    append_info info,
    std::string value,
    bool fillEmptyStringAsNull) {
  append_varchar(column, info, value.data(), fillEmptyStringAsNull);
}

void append_key(int32_t column, append_info info, int64_t value) {
  auto append_info = (tpcds::TpcdsTableDef*)info;
  if (append_info->IsNull(column) || value < 0) {
    append_info->children[append_info->colIndex]->setNull(
        append_info->rowIndex, true);
  } else {
    append_info->children[append_info->colIndex]->asFlatVector<int64_t>()->set(
        append_info->rowIndex, value);
  }
  append_info->colIndex++;
}

void append_integer(int32_t column, append_info info, int32_t value) {
  auto append_info = (tpcds::TpcdsTableDef*)info;
  if (append_info->IsNull(column) || (column == CC_CLOSED_DATE_ID)) {
    append_info->children[append_info->colIndex]->setNull(
        append_info->rowIndex, true);
  } else {
    append_info->children[append_info->colIndex]->asFlatVector<int32_t>()->set(
        append_info->rowIndex, value);
  }
  append_info->colIndex++;
}

void append_boolean(int32_t column, append_info info, int32_t value) {
  auto append_info = (tpcds::TpcdsTableDef*)info;
  if (append_info->IsNull(column)) {
    append_info->children[append_info->colIndex]->setNull(
        append_info->rowIndex, true);
  } else {
    append_info->children[append_info->colIndex]->asFlatVector<int32_t>()->set(
        append_info->rowIndex, value != 0);
  }
  append_info->colIndex++;
}

// value is a Julian date
// FIXME: direct int conversion, offsets should be constant
void append_date(int32_t column, append_info info, int64_t value) {
  auto append_info = (tpcds::TpcdsTableDef*)info;
  if (append_info->IsNull(column) || value < 0) {
    append_info->children[append_info->colIndex]->setNull(
        append_info->rowIndex, true);
  } else {
    date_t dTemp;
    jtodt(&dTemp, (int)value);
    auto stringDate =
        fmt::format("{}-{}-{}", dTemp.year, dTemp.month, dTemp.day);
    auto date = DATE()->toDays(stringDate);
    append_info->children[append_info->colIndex]->asFlatVector<int32_t>()->set(
        append_info->rowIndex, date);
  }
  append_info->colIndex++;
}

void append_decimal(int32_t column, append_info info, decimal_t* val) {
  auto append_info = (tpcds::TpcdsTableDef*)info;
  if (append_info->IsNull(column)) {
    append_info->children[append_info->colIndex]->setNull(
        append_info->rowIndex, true);
  } else {
    auto type = append_info->children[append_info->colIndex]->type();
    if (type->isShortDecimal()) {
      append_info->children[append_info->colIndex]
          ->asFlatVector<int64_t>()
          ->set(append_info->rowIndex, val->number);
    } else {
      append_info->children[append_info->colIndex]
          ->asFlatVector<int128_t>()
          ->set(append_info->rowIndex, val->number);
    }
  }
  append_info->colIndex++;
}

void append_integer_decimal(int32_t column, append_info info, int32_t value) {
  auto append_info = (tpcds::TpcdsTableDef*)info;
  if (append_info->IsNull(column)) {
    append_info->children[append_info->colIndex]->setNull(
        append_info->rowIndex, true);
  } else {
    append_info->children[append_info->colIndex]->asFlatVector<int64_t>()->set(
        append_info->rowIndex, (int64_t)value * 100);
  }
  append_info->colIndex++;
}
