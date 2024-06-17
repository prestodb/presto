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

#include "velox/functions/sparksql/specialforms/SparkCastHooks.h"
#include "velox/functions/lib/string/StringImpl.h"
#include "velox/type/TimestampConversion.h"

namespace facebook::velox::functions::sparksql {

Expected<Timestamp> SparkCastHooks::castStringToTimestamp(
    const StringView& view) const {
  return util::fromTimestampString(
      view.data(), view.size(), util::TimestampParseMode::kSparkCast);
}

Expected<int32_t> SparkCastHooks::castStringToDate(
    const StringView& dateString) const {
  // Allows all patterns supported by Spark:
  // `[+-]yyyy*`
  // `[+-]yyyy*-[m]m`
  // `[+-]yyyy*-[m]m-[d]d`
  // `[+-]yyyy*-[m]m-[d]d *`
  // `[+-]yyyy*-[m]m-[d]dT*`
  // The asterisk `*` in `yyyy*` stands for any numbers.
  // For the last two patterns, the trailing `*` can represent none or any
  // sequence of characters, e.g:
  //   "1970-01-01 123"
  //   "1970-01-01 (BC)"
  return util::fromDateString(
      removeWhiteSpaces(dateString), util::ParseMode::kSparkCast);
}

Expected<float> SparkCastHooks::castStringToReal(const StringView& data) const {
  return util::Converter<TypeKind::REAL>::tryCast(data);
}

Expected<double> SparkCastHooks::castStringToDouble(
    const StringView& data) const {
  return util::Converter<TypeKind::DOUBLE>::tryCast(data);
}

StringView SparkCastHooks::removeWhiteSpaces(const StringView& view) const {
  StringView output;
  stringImpl::trimUnicodeWhiteSpace<true, true, StringView, StringView>(
      output, view);
  return output;
}

const TimestampToStringOptions& SparkCastHooks::timestampToStringOptions()
    const {
  static constexpr TimestampToStringOptions options = {
      .precision = TimestampToStringOptions::Precision::kMicroseconds,
      .leadingPositiveSign = true,
      .skipTrailingZeros = true,
      .zeroPaddingYear = true,
      .dateTimeSeparator = ' ',
  };
  return options;
}

exec::PolicyType SparkCastHooks::getPolicy() const {
  return exec::PolicyType::SparkCastPolicy;
}
} // namespace facebook::velox::functions::sparksql
