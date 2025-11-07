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

class VeloxToPrestoExceptionTranslator {
 public:
  // Translates to Presto error from Velox exceptions
  static protocol::ExecutionFailureInfo translate(
      const velox::VeloxException& e);

  // Translates to Presto error from std::exceptions
  static protocol::ExecutionFailureInfo translate(const std::exception& e);

  // Returns a reference to the error map containing mapping between
  // velox error code and Presto errors defined in Presto protocol
  static std::unordered_map<
      std::string,
      std::unordered_map<std::string, protocol::ErrorCode>>&
  translateMap();
};
} // namespace facebook::presto
