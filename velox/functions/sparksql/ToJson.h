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

#include <vector>
#include "velox/expression/ComplexViewTypes.h"
#include "velox/functions/lib/DateTimeFormatter.h"
#include "velox/functions/lib/TimeUtils.h"
#include "velox/type/DecimalUtil.h"

namespace facebook::velox::functions::sparksql {
namespace detail {

struct JsonOptions {
  const tz::TimeZone* timeZone;

  // Whether to ignore null fields during json generating.
  const bool ignoreNullFields{true};
};

template <typename T>
std::enable_if_t<std::is_floating_point_v<T>, void>
append(T value, std::string& result, bool isMapKey) {
  if (!isMapKey && FOLLY_UNLIKELY(std::isinf(value) || std::isnan(value))) {
    result.append(fmt::format(
        "\"{}\"", util::Converter<TypeKind::VARCHAR>::tryCast(value).value()));
  } else {
    result.append(util::Converter<TypeKind::VARCHAR>::tryCast(value).value());
  }
}

template <typename T>
void appendDecimal(T value, const Type& type, std::string& result) {
  auto [precision, scale] = getDecimalPrecisionScale(type);
  const size_t maxSize = DecimalUtil::maxStringViewSize(precision, scale);
  std::vector<char> buffer(maxSize);
  size_t len = DecimalUtil::castToString(value, scale, maxSize, buffer.data());
  result.append(buffer.data(), len);
}

// Forward declarations for explicit specializations.
template <TypeKind kind>
void toJson(
    const exec::GenericView& input,
    std::string& result,
    const JsonOptions& options,
    bool isMapKey = false);

template <>
void toJson<TypeKind::ROW>(
    const exec::GenericView& input,
    std::string& result,
    const JsonOptions& options,
    bool isMapKey);

template <>
void toJson<TypeKind::ARRAY>(
    const exec::GenericView& input,
    std::string& result,
    const JsonOptions& options,
    bool isMapKey);

template <>
void toJson<TypeKind::MAP>(
    const exec::GenericView& input,
    std::string& result,
    const JsonOptions& options,
    bool isMapKey);

// Primary specialization for unsupported types.
template <TypeKind kind>
void toJson(
    const exec::GenericView& input,
    std::string& result,
    const JsonOptions& options,
    bool isMapKey) {
  VELOX_USER_FAIL("{} is not supported in to_json.", kind);
}

// Convert primitive-type input to Json string.
template <>
void toJson<TypeKind::BOOLEAN>(
    const exec::GenericView& input,
    std::string& result,
    const JsonOptions& /*options*/,
    bool /*isMapKey*/) {
  auto value = input.castTo<bool>();
  constexpr std::string_view kTrue = "true";
  constexpr std::string_view kFalse = "false";
  result.append(value ? kTrue : kFalse);
}

template <>
void toJson<TypeKind::TINYINT>(
    const exec::GenericView& input,
    std::string& result,
    const JsonOptions& /*options*/,
    bool /*isMapKey*/) {
  auto value = input.castTo<int8_t>();
  folly::toAppend<std::string, int8_t>(value, &result);
}

template <>
void toJson<TypeKind::SMALLINT>(
    const exec::GenericView& input,
    std::string& result,
    const JsonOptions& /*options*/,
    bool /*isMapKey*/) {
  auto value = input.castTo<int16_t>();
  folly::toAppend<std::string, int16_t>(value, &result);
}

template <>
void toJson<TypeKind::INTEGER>(
    const exec::GenericView& input,
    std::string& result,
    const JsonOptions& /*options*/,
    bool isMapKey) {
  auto value = input.castTo<int32_t>();
  if (!isMapKey && input.type()->isDate()) {
    result.append("\"").append(DATE()->toString(value)).append("\"");
  } else {
    folly::toAppend<std::string, int32_t>(value, &result);
  }
}

template <>
void toJson<TypeKind::BIGINT>(
    const exec::GenericView& input,
    std::string& result,
    const JsonOptions& /*options*/,
    bool /*isMapKey*/) {
  auto value = input.castTo<int64_t>();
  if (input.type()->isDecimal()) {
    appendDecimal(value, *input.type(), result);
  } else {
    folly::toAppend<std::string, int64_t>(value, &result);
  }
}

template <>
void toJson<TypeKind::HUGEINT>(
    const exec::GenericView& input,
    std::string& result,
    const JsonOptions& /*options*/,
    bool /*isMapKey*/) {
  VELOX_DCHECK(input.type()->isDecimal(), "HUGEINT must be a decimal type.");
  auto value = input.castTo<int128_t>();
  appendDecimal(value, *input.type(), result);
}

template <>
void toJson<TypeKind::REAL>(
    const exec::GenericView& input,
    std::string& result,
    const JsonOptions& /*options*/,
    bool isMapKey) {
  auto value = input.castTo<float>();
  append(value, result, isMapKey);
}

template <>
void toJson<TypeKind::DOUBLE>(
    const exec::GenericView& input,
    std::string& result,
    const JsonOptions& /*options*/,
    bool isMapKey) {
  auto value = input.castTo<double>();
  append(value, result, isMapKey);
}

template <>
void toJson<TypeKind::VARCHAR>(
    const exec::GenericView& input,
    std::string& result,
    const JsonOptions& /*options*/,
    bool isMapKey) {
  auto value = input.castTo<Varchar>();
  if (!isMapKey) {
    folly::json::escapeString(value, result, {});
  } else {
    // toJson<TypeKind::MAP> wraps the key with double quotes.
    // To avoid duplicate quotes, we strip the surrounding quotes after
    // escaping.
    std::string quotedString;
    folly::json::escapeString(value, quotedString, {});
    result.append(quotedString.substr(1, quotedString.size() - 2));
  }
}

template <>
void toJson<TypeKind::TIMESTAMP>(
    const exec::GenericView& input,
    std::string& result,
    const JsonOptions& options,
    bool isMapKey) {
  auto value = input.castTo<Timestamp>();
  if (isMapKey) {
    folly::toAppend<std::string, int64_t>(value.toMicros(), &result);
  } else {
    // Spark converts Timestamp in ISO8601 format by default.
    static const auto formatter =
        functions::buildJodaDateTimeFormatter("yyyy-MM-dd'T'HH:mm:ss.SSSZZ")
            .value();
    const auto maxSize = formatter->maxResultSize(options.timeZone);
    std::vector<char> buffer(maxSize);
    auto size = formatter->format(
        value, options.timeZone, maxSize, buffer.data(), false, "Z");
    result.append("\"").append(buffer.data(), size).append("\"");
  }
}

// Convert complex-type input to Json string.
template <>
void toJson<TypeKind::ROW>(
    const exec::GenericView& input,
    std::string& result,
    const JsonOptions& options,
    bool isMapKey) {
  auto rowType = std::static_pointer_cast<const RowType>(input.type());
  auto row = input.castTo<DynamicRow>();
  result.append(isMapKey ? "[" : "{");
  bool commaBefore = false;
  for (int i = 0; i < rowType->size(); i++) {
    auto data = row.at(i);
    if (data.has_value()) {
      if (commaBefore) {
        result.append(",");
      }
      if (!isMapKey) {
        result.append("\"");
        result.append(rowType->nameOf(i));
        result.append("\":");
      }
      VELOX_DYNAMIC_TYPE_DISPATCH_ALL(
          detail::toJson,
          data->kind(),
          data.value(),
          result,
          options,
          isMapKey);
      commaBefore = true;
    } else if (isMapKey) {
      if (commaBefore) {
        result.append(",");
      }
      result.append("null");
      commaBefore = true;
    } else if (!options.ignoreNullFields) {
      if (commaBefore) {
        result.append(",");
      }
      result.append("\"");
      result.append(rowType->nameOf(i));
      result.append("\":null");
      commaBefore = true;
    }
  }
  result.append(isMapKey ? "]" : "}");
}

template <>
void toJson<TypeKind::ARRAY>(
    const exec::GenericView& input,
    std::string& result,
    const JsonOptions& options,
    bool isMapKey) {
  auto arrayView = input.castTo<Array<Any>>();
  result.append("[");
  for (int i = 0; i < arrayView.size(); i++) {
    if (i > 0) {
      result.append(",");
    }
    if (arrayView[i].has_value()) {
      auto element = arrayView[i].value();
      VELOX_DYNAMIC_TYPE_DISPATCH_ALL(
          detail::toJson, element.kind(), element, result, options, isMapKey);
    } else {
      result.append("null");
    }
  }
  result.append("]");
}

template <>
void toJson<TypeKind::MAP>(
    const exec::GenericView& input,
    std::string& result,
    const JsonOptions& options,
    bool isMapKey) {
  auto mapView = input.castTo<Map<Any, Any>>();
  result.append("{");
  for (int i = 0; i < mapView.size(); i++) {
    if (i > 0) {
      result.append(",");
    }
    auto element = mapView.atIndex(i);
    auto key = element.first;
    auto value = element.second;
    result.append("\"");
    VELOX_DYNAMIC_TYPE_DISPATCH_ALL(
        detail::toJson, key.kind(), key, result, options, true);
    result.append("\":");
    if (value.has_value()) {
      VELOX_DYNAMIC_TYPE_DISPATCH_ALL(
          detail::toJson,
          value.value().kind(),
          value.value(),
          result,
          options,
          isMapKey);
    } else {
      result.append("null");
    }
  }
  result.append("}");
}
} // namespace detail

// ToJsonFunction converts a Json object(ROW, ARRAY, or MAP) to a Json string.
template <typename T>
struct ToJsonFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& inputTypes,
      const core::QueryConfig& config,
      const void* input) {
    initialize(inputTypes, config, input, nullptr);
  }

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& inputTypes,
      const core::QueryConfig& config,
      const void* /*input*/,
      const void* /*timeZone*/) {
    VELOX_USER_CHECK(
        isSupportedType(inputTypes[0], true),
        "to_json function does not support type {}.",
        inputTypes[0]->toString());
    sessionTimezone_ = getTimeZoneFromConfig(config);
    ignoreNullFields_ = config.sparkJsonIgnoreNullFields();
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<Generic<T1>>& input) {
    return callImpl(result, input, sessionTimezone_);
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<Generic<T1>>& input,
      const arg_type<Varchar>& timeZone) {
    auto tz = tz::locateZone(std::string_view(timeZone));
    return callImpl(result, input, tz);
  }

 private:
  // Determine whether a given input type is supported.
  // 1. The root type can only be ROW, ARRAY, and MAP.
  // 2. The key type of MAP cannot be/contain MAP.
  bool isSupportedType(
      const TypePtr& type,
      bool isRootType = false,
      bool isMapKey = false) {
    switch (type->kind()) {
      case TypeKind::ROW: {
        for (const auto& child : asRowType(type)->children()) {
          if (!isSupportedType(child, false, isMapKey)) {
            return false;
          }
        }
        return true;
      }
      case TypeKind::ARRAY: {
        return isSupportedType(type->childAt(0), false, isMapKey);
      }
      case TypeKind::MAP: {
        return !isMapKey && isSupportedType(type->childAt(0), false, true) &&
            isSupportedType(type->childAt(1));
      }
      default:
        return !isRootType;
    }
  }

  FOLLY_ALWAYS_INLINE bool callImpl(
      out_type<Varchar>& result,
      const arg_type<Generic<T1>>& input,
      const tz::TimeZone* timeZone) {
    std::string res;
    VELOX_DYNAMIC_TYPE_DISPATCH_ALL(
        detail::toJson,
        input.kind(),
        input,
        res,
        detail::JsonOptions{
            .timeZone = timeZone, .ignoreNullFields = ignoreNullFields_});
    result.resize(res.size());
    std::memcpy(result.data(), res.c_str(), res.size());
    return true;
  }

  const tz::TimeZone* sessionTimezone_;
  bool ignoreNullFields_;
};

} // namespace facebook::velox::functions::sparksql
