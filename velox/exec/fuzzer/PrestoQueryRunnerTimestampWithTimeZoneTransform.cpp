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

#include "velox/exec/fuzzer/PrestoQueryRunnerTimestampWithTimeZoneTransform.h"
#include "velox/functions/lib/DateTimeFormatter.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"
#include "velox/parse/Expressions.h"
#include "velox/type/tz/TimeZoneMap.h"
#include "velox/vector/SimpleVector.h"

namespace facebook::velox::exec::test {
namespace {
const std::string kFormat = "yyyy-MM-dd HH:mm:ss.SSS ZZZ";
const std::string kBackupFormat = "yyyy-MM-dd HH:mm:ss.SSS ZZ";

std::string format(const int64_t timestampWithTimeZone) {
  static const std::shared_ptr<functions::DateTimeFormatter> kJodaDateTime =
      functions::buildJodaDateTimeFormatter(kFormat).value();

  const auto timestamp = unpackTimestampUtc(timestampWithTimeZone);
  const auto timeZoneId = unpackZoneKeyId(timestampWithTimeZone);
  auto* timezonePtr = tz::locateZone(tz::getTimeZoneName(timeZoneId));

  const auto maxResultSize = kJodaDateTime->maxResultSize(timezonePtr);
  std::string str;
  str.resize(maxResultSize);
  const auto resultSize =
      kJodaDateTime->format(timestamp, timezonePtr, maxResultSize, str.data());
  str.resize(resultSize);

  return str;
}
} // namespace

// Convert a TimestampWithTimeZone into a Varchar using DatetimeFormatter, so
// that we can get the TimestampWithTimeZone back by calling parse_datetime.
variant TimestampWithTimeZoneTransform::transform(
    const BaseVector* const vector,
    vector_size_t row) const {
  VELOX_CHECK(isTimestampWithTimeZoneType(vector->type()));
  VELOX_CHECK(!vector->isNullAt(row));

  return variant::create<TypeKind::VARCHAR>(
      format(vector->asChecked<SimpleVector<int64_t>>()->valueAt(row)));
}

// Applies parse_datetime to a Vector of VARCHAR (formatted timestamps with time
// zone) to produce values of type TimestampWithTimeZone.
core::ExprPtr TimestampWithTimeZoneTransform::projectionExpr(
    const core::ExprPtr& inputExpr,
    const std::string& columnAlias) const {
  // format_datetime with the ZZZ pattern produces time zones that need to be
  // parsed with either the ZZZ or ZZ pattern, to handle this we try parsing
  // with the ZZZ pattern first, and then the ZZ pattern if that fails.
  // coalesce(try(parse_datetime(..., '... ZZZ')), parse_datetime(..., '...
  // ZZ'))
  return std::make_shared<core::CallExpr>(
      "coalesce",
      std::vector<core::ExprPtr>{
          std::make_shared<core::CallExpr>(
              "try",
              std::vector<core::ExprPtr>{std::make_shared<core::CallExpr>(
                  "parse_datetime",
                  std::vector<core::ExprPtr>{
                      inputExpr,
                      std::make_shared<core::ConstantExpr>(
                          VARCHAR(),
                          variant::create<TypeKind::VARCHAR>(kFormat),
                          std::nullopt)},
                  std::nullopt)},
              std::nullopt),
          std::make_shared<core::CallExpr>(
              "parse_datetime",
              std::vector<core::ExprPtr>{
                  inputExpr,
                  std::make_shared<core::ConstantExpr>(
                      VARCHAR(),
                      variant::create<TypeKind::VARCHAR>(kBackupFormat),
                      std::nullopt)},
              std::nullopt)},
      columnAlias);
}
} // namespace facebook::velox::exec::test
