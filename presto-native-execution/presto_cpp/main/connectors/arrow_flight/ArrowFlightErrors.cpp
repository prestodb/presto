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
#include "presto_cpp/main/connectors/arrow_flight/ArrowFlightErrors.h"
#include <arrow/flight/api.h>
#include <arrow/status.h>
#include <fmt/format.h>
#include <charconv>
#include <optional>
#include <sstream>
#include <string>
#include "presto_cpp/main/common/Counters.h"
#include "presto_cpp/main/common/Exception.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/base/StatsReporter.h"

namespace facebook::presto {
namespace {

// Categories mirror Presto ErrorType so a metric bucket always agrees with the
// error the query ultimately fails with.
constexpr std::string_view kCategoryUserError = "user_error";
constexpr std::string_view kCategoryExternal = "external";
constexpr std::string_view kCategoryInsufficientResources =
    "insufficient_resources";
constexpr std::string_view kCategoryInternalError = "internal_error";

// Keys in the FlightStatusDetail extra info payload that a Flight server may
// attach to describe the Presto error behind a failed call.
constexpr std::string_view kPrestoErrorNameKey = "presto-error-name";
constexpr std::string_view kPrestoErrorCodeKey = "presto-error-code";
constexpr std::string_view kPrestoErrorTypeKey = "presto-error-type";
constexpr std::string_view kPrestoErrorRetriableKey = "presto-error-retriable";

constexpr std::string_view kErrorTypeUserError = "USER_ERROR";
constexpr std::string_view kErrorTypeExternal = "EXTERNAL";
constexpr std::string_view kErrorTypeInsufficientResources =
    "INSUFFICIENT_RESOURCES";
constexpr std::string_view kErrorTypeInternalError = "INTERNAL_ERROR";

struct PrestoErrorDetails {
  std::string name;
  std::optional<int> code;
  std::string type;
  bool retriable{false};
};

std::optional<PrestoErrorDetails> parsePrestoErrorDetails(
    const arrow::Status& status) {
  auto detail = arrow::flight::FlightStatusDetail::UnwrapStatus(status);
  if (detail == nullptr || detail->extra_info().empty()) {
    return std::nullopt;
  }

  PrestoErrorDetails result;
  std::istringstream payload{detail->extra_info()};
  std::string line;
  while (std::getline(payload, line)) {
    const auto pos = line.find('=');
    if (pos == std::string::npos) {
      continue;
    }
    const std::string_view key = std::string_view(line).substr(0, pos);
    const auto value = line.substr(pos + 1);
    if (key == kPrestoErrorNameKey) {
      result.name = value;
    } else if (key == kPrestoErrorCodeKey) {
      int code = 0;
      const auto [ptr, ec] =
          std::from_chars(value.data(), value.data() + value.size(), code);
      if (ec == std::errc() && ptr == value.data() + value.size()) {
        result.code = code;
      }
    } else if (key == kPrestoErrorTypeKey) {
      result.type = value;
    } else if (key == kPrestoErrorRetriableKey) {
      result.retriable = (value == "true");
    }
  }

  if (result.type.empty()) {
    return std::nullopt;
  }
  return result;
}

} // namespace

std::string_view errorCategory(const arrow::Status& status) {
  if (auto details = parsePrestoErrorDetails(status)) {
    if (details->type == kErrorTypeUserError) {
      return kCategoryUserError;
    }
    if (details->type == kErrorTypeInsufficientResources) {
      return kCategoryInsufficientResources;
    }
    if (details->type == kErrorTypeExternal) {
      return kCategoryExternal;
    }
    return kCategoryInternalError;
  }

  if (auto detail = arrow::flight::FlightStatusDetail::UnwrapStatus(status)) {
    switch (detail->code()) {
      case arrow::flight::FlightStatusCode::TimedOut:
      case arrow::flight::FlightStatusCode::Unavailable:
      case arrow::flight::FlightStatusCode::Unauthenticated:
      case arrow::flight::FlightStatusCode::Unauthorized:
        return kCategoryExternal;
      default:
        return kCategoryInternalError;
    }
  }

  switch (status.code()) {
    case arrow::StatusCode::OutOfMemory:
    case arrow::StatusCode::CapacityError:
      return kCategoryInsufficientResources;
    case arrow::StatusCode::IOError:
      return kCategoryExternal;
    default:
      return kCategoryInternalError;
  }
}

std::string_view currentExceptionCategory() {
  try {
    throw;
  } catch (const std::bad_alloc&) {
    return kCategoryInsufficientResources;
  } catch (const velox::VeloxException& e) {
    if (e.errorCode() == "MEM_CAP_EXCEEDED" ||
        e.errorCode() == "MEM_ARBITRATION_FAILURE" ||
        e.errorCode() == "MEM_ARBITRATION_TIMEOUT" ||
        e.errorCode() == "MEM_ALLOC_ERROR" || e.errorCode() == "MEM_ABORTED" ||
        e.errorCode() == "NO_CACHE_SPACE") {
      return kCategoryInsufficientResources;
    }
    if (e.errorSource() == "USER") {
      return kCategoryUserError;
    }
    if (e.errorSource() == "EXTERNAL") {
      return kCategoryExternal;
    }
    return kCategoryInternalError;
  } catch (...) {
    return kCategoryInternalError;
  }
}

void recordCategoryCounter(std::string_view category) {
  if (category == kCategoryUserError) {
    RECORD_METRIC_VALUE(kCounterArrowFlightUserErrors, 1);
  } else if (category == kCategoryExternal) {
    RECORD_METRIC_VALUE(kCounterArrowFlightExternalErrors, 1);
  } else if (category == kCategoryInsufficientResources) {
    RECORD_METRIC_VALUE(kCounterArrowFlightInsufficientResourcesErrors, 1);
  } else {
    RECORD_METRIC_VALUE(kCounterArrowFlightInternalErrors, 1);
  }
}

void raiseFlightError(const arrow::Status& status) {
  const auto& message = status.message();

  if (auto details = parsePrestoErrorDetails(status)) {
    const auto detailedMessage = fmt::format("{}: {}", details->name, message);
    // A complete tuple (name + code) is passed through verbatim so the
    // translator reproduces the exact downstream Presto error code.
    const bool passthrough = details->code.has_value() && !details->name.empty();
    const auto passthroughCode = passthrough
        ? passthrough_error::encode(
              details->name, *details->code, details->type, details->retriable)
        : std::string();
    if (details->type == kErrorTypeUserError) {
      throw velox::VeloxUserError(
          __FILE__,
          __LINE__,
          __FUNCTION__,
          "",
          detailedMessage,
          velox::error_source::kErrorSourceUser,
          passthrough ? passthroughCode
                      : std::string(velox::error_code::kInvalidArgument),
          false);
    }
    if (details->type == kErrorTypeInsufficientResources) {
      throw velox::VeloxRuntimeError(
          __FILE__,
          __LINE__,
          __FUNCTION__,
          "",
          detailedMessage,
          velox::error_source::kErrorSourceRuntime,
          passthrough
              ? passthroughCode
              : std::string(presto_error_name::kArrowFlightResourceErrorName),
          false);
    }
    if (details->type == kErrorTypeExternal) {
      throw velox::VeloxExternalError(
          __FILE__,
          __LINE__,
          __FUNCTION__,
          "",
          detailedMessage,
          velox::error_source::kErrorSourceExternal,
          passthrough
              ? passthroughCode
              : std::string(presto_error_name::kArrowFlightRemoteErrorName),
          details->retriable);
    }
    throw velox::VeloxRuntimeError(
        __FILE__,
        __LINE__,
        __FUNCTION__,
        "",
        detailedMessage,
        velox::error_source::kErrorSourceRuntime,
        (passthrough && details->type == kErrorTypeInternalError)
            ? passthroughCode
            : std::string(presto_error_name::kArrowFlightInternalErrorName),
        false);
  }

  if (auto detail = arrow::flight::FlightStatusDetail::UnwrapStatus(status)) {
    switch (detail->code()) {
      case arrow::flight::FlightStatusCode::TimedOut:
      case arrow::flight::FlightStatusCode::Unavailable:
        throw velox::VeloxExternalError(
            __FILE__,
            __LINE__,
            __FUNCTION__,
            "",
            message,
            velox::error_source::kErrorSourceExternal,
            presto_error_name::kArrowFlightUnavailableErrorName,
            true);
      case arrow::flight::FlightStatusCode::Unauthenticated:
      case arrow::flight::FlightStatusCode::Unauthorized:
        throw velox::VeloxExternalError(
            __FILE__,
            __LINE__,
            __FUNCTION__,
            "",
            message,
            velox::error_source::kErrorSourceExternal,
            presto_error_name::kArrowFlightAuthErrorName,
            false);
      default:
        throw velox::VeloxRuntimeError(
            __FILE__,
            __LINE__,
            __FUNCTION__,
            "",
            message,
            velox::error_source::kErrorSourceRuntime,
            presto_error_name::kArrowFlightInternalErrorName,
            false);
    }
  }

  switch (status.code()) {
    case arrow::StatusCode::OutOfMemory:
    case arrow::StatusCode::CapacityError:
      throw velox::VeloxRuntimeError(
          __FILE__,
          __LINE__,
          __FUNCTION__,
          "",
          message,
          velox::error_source::kErrorSourceRuntime,
          presto_error_name::kArrowFlightResourceErrorName,
          false);
    case arrow::StatusCode::IOError:
      throw velox::VeloxExternalError(
          __FILE__,
          __LINE__,
          __FUNCTION__,
          "",
          message,
          velox::error_source::kErrorSourceExternal,
          presto_error_name::kArrowFlightUnavailableErrorName,
          true);
    default:
      throw velox::VeloxRuntimeError(
          __FILE__,
          __LINE__,
          __FUNCTION__,
          "",
          message,
          velox::error_source::kErrorSourceRuntime,
          presto_error_name::kArrowFlightInternalErrorName,
          false);
  }
}

} // namespace facebook::presto
