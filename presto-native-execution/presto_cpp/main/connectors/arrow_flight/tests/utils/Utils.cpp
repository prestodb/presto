/*
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
#include "Utils.h"
#include <arrow/status.h>
#include <vector>
#include "velox/type/StringView.h"

namespace facebook::presto::test {

ArrowArrayPtr makeDecimalArray(
    const std::vector<int64_t>& decimalValues,
    int precision,
    int scale) {
  auto decimalType = arrow::decimal(precision, scale);
  auto builder =
      arrow::Decimal128Builder(decimalType, arrow::default_memory_pool());

  for (const auto& value : decimalValues) {
    arrow::Decimal128 dec(value);
    AFC_RAISE_NOT_OK(builder.Append(dec));
  }
  AFC_RETURN_OR_RAISE(builder.Finish());
}

ArrowArrayPtr makeTimestampArray(
    const std::vector<int64_t>& values,
    arrow::TimeUnit::type timeUnit,
    arrow::MemoryPool* memory_pool) {
  arrow::TimestampBuilder builder(arrow::timestamp(timeUnit), memory_pool);
  AFC_RAISE_NOT_OK(builder.AppendValues(values));
  AFC_RETURN_OR_RAISE(builder.Finish());
}

std::vector<facebook::velox::StringView> makeStringViewVector(
    const std::vector<std::string>& values) {
  std::vector<facebook::velox::StringView> stringViewVector;
  stringViewVector.reserve(values.size());
  for (const auto& value : values) {
    stringViewVector.emplace_back(value);
  }
  return stringViewVector;
}

ArrowArrayPtr makeStringArray(const std::vector<std::string>& values) {
  auto builder = arrow::StringBuilder{};
  AFC_RAISE_NOT_OK(builder.AppendValues(values));
  AFC_RETURN_OR_RAISE(builder.Finish());
}

ArrowArrayPtr makeBinaryArray(const std::vector<std::string>& values) {
  auto builder = arrow::BinaryBuilder{};
  AFC_RAISE_NOT_OK(builder.AppendValues(values));
  AFC_RETURN_OR_RAISE(builder.Finish());
}

ArrowArrayPtr makeBooleanArray(const std::vector<bool>& values) {
  auto builder = arrow::BooleanBuilder{};
  AFC_RAISE_NOT_OK(builder.AppendValues(values));
  AFC_RETURN_OR_RAISE(builder.Finish());
}

std::shared_ptr<arrow::RecordBatch> makeRecordBatch(
    const std::vector<std::string>& names,
    const arrow::ArrayVector& arrays) {
  VELOX_CHECK_EQ(names.size(), arrays.size());

  auto numRows = (!arrays.empty()) ? (arrays[0]->length()) : 0;
  arrow::FieldVector fields{};
  for (int i = 0; i < arrays.size(); i++) {
    VELOX_CHECK_EQ(arrays[i]->length(), numRows);
    fields.push_back(
        std::make_shared<arrow::Field>(names[i], arrays[i]->type()));
  }

  auto schema = arrow::schema(fields);
  return arrow::RecordBatch::Make(schema, numRows, arrays);
}

std::shared_ptr<arrow::Table> makeArrowTable(
    const std::vector<std::string>& names,
    const arrow::ArrayVector& arrays) {
  AFC_RETURN_OR_RAISE(
      arrow::Table::FromRecordBatches({makeRecordBatch(names, arrays)}));
}

std::string readFile(const std::string& path) {
  std::ifstream file(path);
  VELOX_CHECK(
      file.is_open(), "Could not open file \"{}\": {}", path, strerror(errno));
  return {
      std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>()};
}

} // namespace facebook::presto::test
