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
#include "presto_cpp/presto_protocol/presto_protocol.h"
#include "velox/common/base/VeloxException.h"

namespace std {
class exception;
}

namespace facebook::presto {
namespace protocol {
struct ExecutionFailureInfo;
struct ErrorCode;
} // namespace protocol

class VeloxToPrestoExceptionTranslator {
 public:
  // Translates to Presto error from Velox exceptions
  static protocol::ExecutionFailureInfo translate(
      const velox::VeloxException& e);

  // Translates to Presto error from std::exceptions
  static protocol::ExecutionFailureInfo translate(const std::exception& e);

 private:
  static const std::unordered_map<
      std::string,
      std::unordered_map<std::string, protocol::ErrorCode>>&
  translateMap() {
    static const std::unordered_map<
        std::string,
        std::unordered_map<std::string, protocol::ErrorCode>>
        kTranslateMap = {
            {velox::error_source::kErrorSourceRuntime,
             {{velox::error_code::kMemCapExceeded,
               {0x00020000,
                "GENERIC_INSUFFICIENT_RESOURCES",
                protocol::ErrorType::INSUFFICIENT_RESOURCES}},
              {velox::error_code::kInvalidState,
               {0x00010000,
                "GENERIC_INTERNAL_ERROR",
                protocol::ErrorType::INTERNAL_ERROR}},
              {velox::error_code::kUnreachableCode,
               {0x00010000,
                "GENERIC_INTERNAL_ERROR",
                protocol::ErrorType::INTERNAL_ERROR}},
              {velox::error_code::kNotImplemented,
               {0x00010000,
                "GENERIC_INTERNAL_ERROR",
                protocol::ErrorType::INTERNAL_ERROR}},
              {velox::error_code::kUnknown,
               {0x00010000,
                "GENERIC_INTERNAL_ERROR",
                protocol::ErrorType::INTERNAL_ERROR}}}},
            {velox::error_source::kErrorSourceUser,
             {{velox::error_code::kInvalidArgument,
               {0x00000000,
                "GENERIC_USER_ERROR",
                protocol::ErrorType::USER_ERROR}},
              {velox::error_code::kUnsupported,
               {0x0000000D, "NOT_SUPPORTED", protocol::ErrorType::USER_ERROR}},
              {velox::error_code::kArithmeticError,
               {0x00000000,
                "GENERIC_USER_ERROR",
                protocol::ErrorType::USER_ERROR}}}},
            {velox::error_source::kErrorSourceSystem, {}}};
    return kTranslateMap;
  }
};
} // namespace facebook::presto
