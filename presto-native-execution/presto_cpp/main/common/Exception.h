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
#pragma once

#include <charconv>
#include <exception>
#include <optional>
#include <unordered_map>

#include <folly/Singleton.h>

#include "presto_cpp/presto_protocol/core/presto_protocol_core.h"
#include "velox/common/base/VeloxException.h"

namespace facebook::presto {
namespace protocol {
struct ExecutionFailureInfo;
struct ErrorCode;
} // namespace protocol

namespace presto_error_code {

// Ref: presto-common-arrow/src/main/java/com/facebook/plugin/arrow/ArrowErrorCode.java
inline constexpr auto kArrowFlightErrorCodeMask = 0x5100000;
inline constexpr auto kArrowFlightRemoteErrorCode = kArrowFlightErrorCodeMask + 7;
inline constexpr auto kArrowFlightUnavailableErrorCode = kArrowFlightErrorCodeMask + 8;
inline constexpr auto kArrowFlightAuthErrorCode = kArrowFlightErrorCodeMask + 9;
inline constexpr auto kArrowFlightInternalErrorCode = kArrowFlightErrorCodeMask + 10;
inline constexpr auto kArrowFlightResourceErrorCode = kArrowFlightErrorCodeMask + 11;

}

namespace presto_error_name {

// Ref: presto-common-arrow/src/main/java/com/facebook/plugin/arrow/ArrowErrorCode.java
inline constexpr auto kArrowFlightRemoteErrorName =
    "ARROW_FLIGHT_REMOTE_ERROR"_fs;
inline constexpr auto kArrowFlightUnavailableErrorName =
    "ARROW_FLIGHT_UNAVAILABLE_ERROR"_fs;
inline constexpr auto kArrowFlightAuthErrorName = "ARROW_FLIGHT_AUTH_ERROR"_fs;
inline constexpr auto kArrowFlightInternalErrorName =
    "ARROW_FLIGHT_INTERNAL_ERROR"_fs;
inline constexpr auto kArrowFlightResourceErrorName =
    "ARROW_FLIGHT_RESOURCE_ERROR"_fs;

}

namespace error_code {
using namespace folly::string_literals;

/// An error raised when Presto broadcast join exceeds the broadcast size limit.
inline constexpr auto kExceededLocalBroadcastJoinMemoryLimit =
    "EXCEEDED_LOCAL_BROADCAST_JOIN_MEMORY_LIMIT"_fs;
} // namespace error_code

// Carries an exact downstream Presto ErrorCode tuple through a VeloxException
// errorCode string, so the translator can reproduce it verbatim without
// per-connector registrations.
namespace passthrough_error {

inline constexpr std::string_view kPrefix = "PRESTO_PASSTHROUGH:";

/// Encodes an ErrorCode tuple as "PRESTO_PASSTHROUGH:<type>:<0|1>:<code>:<name>".
inline std::string encode(
    std::string_view name,
    int code,
    std::string_view type,
    bool retriable) {
  std::string encoded{kPrefix};
  encoded.append(type);
  encoded.append(retriable ? ":1:" : ":0:");
  encoded.append(std::to_string(code));
  encoded.push_back(':');
  encoded.append(name);
  return encoded;
}

/// Decodes a string produced by encode(). Returns std::nullopt for strings
/// without the pass-through prefix or with malformed fields.
inline std::optional<protocol::ErrorCode> decode(std::string_view encoded) {
  if (encoded.substr(0, kPrefix.size()) != kPrefix) {
    return std::nullopt;
  }
  auto rest = encoded.substr(kPrefix.size());

  const auto typeEnd = rest.find(':');
  if (typeEnd == std::string_view::npos) {
    return std::nullopt;
  }
  const auto typeStr = rest.substr(0, typeEnd);
  rest.remove_prefix(typeEnd + 1);

  const auto retriableEnd = rest.find(':');
  if (retriableEnd == std::string_view::npos) {
    return std::nullopt;
  }
  const auto retriableStr = rest.substr(0, retriableEnd);
  rest.remove_prefix(retriableEnd + 1);

  const auto codeEnd = rest.find(':');
  if (codeEnd == std::string_view::npos) {
    return std::nullopt;
  }
  const auto codeStr = rest.substr(0, codeEnd);
  const auto name = rest.substr(codeEnd + 1);
  if (name.empty()) {
    return std::nullopt;
  }

  protocol::ErrorCode result;
  if (typeStr == "USER_ERROR") {
    result.type = protocol::ErrorType::USER_ERROR;
  } else if (typeStr == "EXTERNAL") {
    result.type = protocol::ErrorType::EXTERNAL;
  } else if (typeStr == "INSUFFICIENT_RESOURCES") {
    result.type = protocol::ErrorType::INSUFFICIENT_RESOURCES;
  } else if (typeStr == "INTERNAL_ERROR") {
    result.type = protocol::ErrorType::INTERNAL_ERROR;
  } else {
    return std::nullopt;
  }

  int code = 0;
  const auto [ptr, ec] =
      std::from_chars(codeStr.data(), codeStr.data() + codeStr.size(), code);
  if (ec != std::errc() || ptr != codeStr.data() + codeStr.size()) {
    return std::nullopt;
  }
  result.code = code;
  result.retriable = (retriableStr == "1");
  result.name = std::string(name);
  return result;
}

} // namespace passthrough_error

// Exception translator singleton for converting Velox exceptions to Presto
// errors. This follows the same pattern as velox/common/base/StatsReporter.h.
//
// IMPORTANT: folly::Singleton enforces single registration per type.
// - Only ONE registration of VeloxToPrestoExceptionTranslator can exist
// - Duplicate registrations will cause program to fail during static init
// - Extended servers must register a derived class
class VeloxToPrestoExceptionTranslator {
 public:
  using ErrorCodeMap = std::unordered_map<
      std::string,
      std::unordered_map<std::string, protocol::ErrorCode>>;

  VeloxToPrestoExceptionTranslator();

  virtual ~VeloxToPrestoExceptionTranslator() = default;

  virtual protocol::ExecutionFailureInfo translate(
      const velox::VeloxException& e) const;

  virtual protocol::ExecutionFailureInfo translate(
      const std::exception& e) const;

  // For testing purposes only - provides access to the error map
  const ErrorCodeMap& testingErrorMap() const {
    return errorMap_;
  }

 protected:
  void registerError(
      const std::string& errorSource,
      const std::string& errorCode,
      const protocol::ErrorCode& prestoErrorCode);

  ErrorCodeMap errorMap_;
};

// Global inline function APIs to translate exceptions (returns
// ExecutionFailureInfo) Similar pattern to StatsReporter, but returns a value
// instead of recording
inline protocol::ExecutionFailureInfo translateToPrestoException(
    const velox::VeloxException& e) {
  const auto translator =
      folly::Singleton<VeloxToPrestoExceptionTranslator>::try_get_fast();
  VELOX_CHECK_NOT_NULL(
      translator,
      "VeloxToPrestoExceptionTranslator singleton must be registered");
  return translator->translate(e);
}

inline protocol::ExecutionFailureInfo translateToPrestoException(
    const std::exception& e) {
  const auto translator =
      folly::Singleton<VeloxToPrestoExceptionTranslator>::try_get_fast();
  VELOX_CHECK_NOT_NULL(
      translator,
      "VeloxToPrestoExceptionTranslator singleton must be registered");
  return translator->translate(e);
}

protocol::NativeSidecarFailureInfo toNativeSidecarFailureInfo(
    const protocol::ExecutionFailureInfo& failure);
} // namespace facebook::presto
