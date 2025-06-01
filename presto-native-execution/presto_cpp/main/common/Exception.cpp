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

#include "presto_cpp/main/common/Exception.h"

namespace facebook::presto {

std::unordered_map<
    std::string,
    std::unordered_map<std::string, protocol::ErrorCode>>&
VeloxToPrestoExceptionTranslator::translateMap() {
  static std::unordered_map<
      std::string,
      std::unordered_map<std::string, protocol::ErrorCode>>
      errorMap = {
          {velox::error_source::kErrorSourceRuntime,
           {{velox::error_code::kMemCapExceeded,
             {0x00020007,
              "EXCEEDED_LOCAL_MEMORY_LIMIT",
              protocol::ErrorType::INSUFFICIENT_RESOURCES}},

            {velox::error_code::kMemAborted,
             {0x00020000,
              "GENERIC_INSUFFICIENT_RESOURCES",
              protocol::ErrorType::INSUFFICIENT_RESOURCES}},

            {velox::error_code::kSpillLimitExceeded,
             {0x00020006,
              "EXCEEDED_SPILL_LIMIT",
              protocol::ErrorType::INSUFFICIENT_RESOURCES}},

            {velox::error_code::kMemArbitrationFailure,
             {0x00020000,
              "MEMORY_ARBITRATION_FAILURE",
              protocol::ErrorType::INSUFFICIENT_RESOURCES}},

            {velox::error_code::kMemArbitrationTimeout,
             {0x00020000,
              "GENERIC_INSUFFICIENT_RESOURCES",
              protocol::ErrorType::INSUFFICIENT_RESOURCES}},

            {velox::error_code::kMemAllocError,
             {0x00020000,
              "GENERIC_INSUFFICIENT_RESOURCES",
              protocol::ErrorType::INSUFFICIENT_RESOURCES}},

            {velox::error_code::kInvalidState,
             {0x00010000,
              "GENERIC_INTERNAL_ERROR",
              protocol::ErrorType::INTERNAL_ERROR}},

            {velox::error_code::kGenericSpillFailure,
             {0x00010023,
              "GENERIC_SPILL_FAILURE",
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
            {velox::error_code::kUnsupportedInputUncatchable,
             {0x0000000D, "NOT_SUPPORTED", protocol::ErrorType::USER_ERROR}},
            {velox::error_code::kArithmeticError,
             {0x00000000,
              "GENERIC_USER_ERROR",
              protocol::ErrorType::USER_ERROR}}}},

          {velox::error_source::kErrorSourceSystem, {}}};
  return errorMap;
}

protocol::ExecutionFailureInfo VeloxToPrestoExceptionTranslator::translate(
    const velox::VeloxException& e) {
  protocol::ExecutionFailureInfo error;
  // Line number must be >= 1
  error.errorLocation.lineNumber = e.line() >= 1 ? e.line() : 1;
  error.errorLocation.columnNumber = 1;
  error.type = e.exceptionName();
  std::stringstream msg;
  msg << e.failingExpression() << " " << e.message();
  if (!e.context().empty()) {
    msg << " " << e.context();
  }
  if (!e.additionalContext().empty()) {
    msg << " " << e.additionalContext();
  }
  error.message = msg.str();
  // Stack trace may not be available if stack trace capturing is disabled or
  // rate limited.
  if (e.stackTrace()) {
    error.stack = e.stackTrace()->toStrVector();
  }

  const auto& errorSource = e.errorSource();
  const auto& errorCode = e.errorCode();

  auto itrErrorCodesMap = translateMap().find(errorSource);
  if (itrErrorCodesMap != translateMap().end()) {
    auto itrErrorCode = itrErrorCodesMap->second.find(errorCode);
    if (itrErrorCode != itrErrorCodesMap->second.end()) {
      error.errorCode = itrErrorCode->second;
      return error;
    }
  }
  error.errorCode.code = 0x00010000;
  error.errorCode.name = "GENERIC_INTERNAL_ERROR";
  error.errorCode.type = protocol::ErrorType::INTERNAL_ERROR;
  return error;
}

protocol::ExecutionFailureInfo VeloxToPrestoExceptionTranslator::translate(
    const std::exception& e) {
  protocol::ExecutionFailureInfo error;
  error.errorLocation.lineNumber = 1;
  error.errorLocation.columnNumber = 1;
  error.errorCode.code = 0x00010000;
  error.errorCode.name = "GENERIC_INTERNAL_ERROR";
  error.errorCode.type = protocol::ErrorType::INTERNAL_ERROR;
  error.type = "std::exception";
  error.message = e.what();
  return error;
}
} // namespace facebook::presto
