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

#include "velox/functions/lib/DateTimeFormatter.h"
#include "velox/functions/lib/TimeUtils.h"
#include "velox/type/tz/TimeZoneMap.h"

namespace facebook::velox::functions::sparksql {

template <typename T>
struct YearFunction : public InitSessionTimezone<T> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE int32_t getYear(const std::tm& time) {
    return 1900 + time.tm_year;
  }

  FOLLY_ALWAYS_INLINE void call(
      int32_t& result,
      const arg_type<Timestamp>& timestamp) {
    result = getYear(getDateTime(timestamp, this->timeZone_));
  }

  FOLLY_ALWAYS_INLINE void call(int32_t& result, const arg_type<Date>& date) {
    result = getYear(getDateTime(date));
  }
};

template <typename T>
struct UnixTimestampFunction {
  // unix_timestamp();
  // If no parameters, return the current unix timestamp without adjusting
  // timezones.
  FOLLY_ALWAYS_INLINE void call(int64_t& result) {
    result = Timestamp::now().getSeconds();
  }
};

template <typename T>
struct UnixTimestampParseFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // unix_timestamp(input);
  // If format is not specified, assume kDefaultFormat.
  FOLLY_ALWAYS_INLINE void initialize(
      const core::QueryConfig& config,
      const arg_type<Varchar>* /*input*/) {
    format_ = buildJodaDateTimeFormatter(kDefaultFormat_);
    setTimezone(config);
  }

  FOLLY_ALWAYS_INLINE bool call(
      int64_t& result,
      const arg_type<Varchar>& input) {
    DateTimeResult dateTimeResult;
    try {
      dateTimeResult =
          format_->parse(std::string_view(input.data(), input.size()));
    } catch (const VeloxUserError&) {
      // Return null if could not parse.
      return false;
    }
    dateTimeResult.timestamp.toGMT(getTimezoneId(dateTimeResult));
    result = dateTimeResult.timestamp.getSeconds();
    return true;
  }

 protected:
  void setTimezone(const core::QueryConfig& config) {
    auto sessionTzName = config.sessionTimezone();
    if (!sessionTzName.empty()) {
      sessionTzID_ = util::getTimeZoneID(sessionTzName);
    }
  }

  int16_t getTimezoneId(const DateTimeResult& result) {
    // If timezone was not parsed, fallback to the session timezone. If there's
    // no session timezone, fallback to 0 (GMT).
    return result.timezoneId != -1 ? result.timezoneId
                                   : sessionTzID_.value_or(0);
  }

  // Default if format is not specified, as per Spark documentation.
  constexpr static std::string_view kDefaultFormat_{"yyyy-MM-dd HH:mm:ss"};
  std::shared_ptr<DateTimeFormatter> format_;
  std::optional<int64_t> sessionTzID_;
};

template <typename T>
struct UnixTimestampParseWithFormatFunction
    : public UnixTimestampParseFunction<T> {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // unix_timestamp(input, format):
  // If format is constant, compile it just once per batch.
  FOLLY_ALWAYS_INLINE void initialize(
      const core::QueryConfig& config,
      const arg_type<Varchar>* /*input*/,
      const arg_type<Varchar>* format) {
    if (format != nullptr) {
      try {
        this->format_ = buildJodaDateTimeFormatter(
            std::string_view(format->data(), format->size()));
      } catch (const VeloxUserError&) {
        invalidFormat_ = true;
      }
      isConstFormat_ = true;
    }
    this->setTimezone(config);
  }

  FOLLY_ALWAYS_INLINE bool call(
      int64_t& result,
      const arg_type<Varchar>& input,
      const arg_type<Varchar>& format) {
    if (invalidFormat_) {
      return false;
    }

    // Format or parsing error returns null.
    try {
      if (!isConstFormat_) {
        this->format_ = buildJodaDateTimeFormatter(
            std::string_view(format.data(), format.size()));
      }

      auto dateTimeResult =
          this->format_->parse(std::string_view(input.data(), input.size()));
      dateTimeResult.timestamp.toGMT(this->getTimezoneId(dateTimeResult));
      result = dateTimeResult.timestamp.getSeconds();
    } catch (const VeloxUserError&) {
      return false;
    }
    return true;
  }

 private:
  bool isConstFormat_{false};
  bool invalidFormat_{false};
};

} // namespace facebook::velox::functions::sparksql
