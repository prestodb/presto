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

#include <folly/Singleton.h>
#include <unordered_map>
#include "presto_cpp/presto_protocol/core/presto_protocol_core.h"
#include "velox/common/base/VeloxException.h"

namespace std {
class exception;
}

namespace facebook::presto {
namespace protocol {
struct ExecutionFailureInfo;
struct ErrorCode;
} // namespace protocol

namespace error_code {
using namespace folly::string_literals;

/// An error raised when Presto broadcast join exceeds the broadcast size limit.
inline constexpr auto kExceededLocalBroadcastJoinMemoryLimit =
    "EXCEEDED_LOCAL_BROADCAST_JOIN_MEMORY_LIMIT"_fs;
} // namespace error_code

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
