#include "append_info-c.hpp"

#include <fmt/format.h>
#include "append_info.h"
#include "config.h"
#include "date.h"
#include "iostream"
#include "nulls.h"
#include "porting.h"

append_info* append_info_get(void* info_list, int table_id) {
  auto& append_vector =
      *((std::vector<std::unique_ptr<tpcds::tpcds_table_def>>*)info_list);
  return (append_info*)append_vector[table_id].get();
}

bool facebook::velox::tpcds::tpcds_table_def::IsNull() {
  return nullCheck(first_column + colIndex);
}

void append_row_start(append_info info) {
  auto append_info = (tpcds::tpcds_table_def*)info;
}

void append_row_end(append_info info) {
  auto append_info = (tpcds::tpcds_table_def*)info;
  append_info->colIndex %= append_info->children.size();
  append_info->rowIndex++;
}

void append_varchar(append_info info, const char* value) {
  auto append_info = (tpcds::tpcds_table_def*)info;
  if (append_info->IsNull()) {
    append_info->children[append_info->colIndex]->setNull(
        append_info->rowIndex, true);
  } else {
    if (value) {
      append_info->children[append_info->colIndex]
          ->asFlatVector<StringView>()
          ->set(append_info->rowIndex, value);
    } else {
      append_info->children[append_info->colIndex]
          ->asFlatVector<StringView>()
          ->setNull(append_info->rowIndex, true);
    }
  }
  append_info->colIndex++;
}

void append_varchar(append_info info, std::string value) {
  append_varchar(info, value.data());
}

void append_key(append_info info, int32_t value) {
  auto append_info = (tpcds::tpcds_table_def*)info;
  if (append_info->IsNull() || value < 0) {
    append_info->children[append_info->colIndex]->setNull(
        append_info->rowIndex, true);
  } else {
    append_info->children[append_info->colIndex]->asFlatVector<int32_t>()->set(
        append_info->rowIndex, value);
  }
  append_info->colIndex++;
}

void append_integer(append_info info, int32_t value) {
  auto append_info = (tpcds::tpcds_table_def*)info;
  if (append_info->IsNull()) {
    append_info->children[append_info->colIndex]->setNull(
        append_info->rowIndex, true);
  } else {
    append_info->children[append_info->colIndex]->asFlatVector<int32_t>()->set(
        append_info->rowIndex, value);
  }
  append_info->colIndex++;
}

void append_boolean(append_info info, int32_t value) {
  auto append_info = (tpcds::tpcds_table_def*)info;
  if (append_info->IsNull()) {
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
void append_date(append_info info, int64_t value) {
  auto append_info = (tpcds::tpcds_table_def*)info;
  if (append_info->IsNull() || value < 0) {
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

void append_decimal(append_info info, decimal_t* val) {
  auto append_info = (tpcds::tpcds_table_def*)info;
  if (append_info->IsNull()) {
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