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

VeloxToPrestoExceptionTranslator::VeloxToPrestoExceptionTranslator() {
  // Register runtime errors
  registerError(
      velox::error_source::kErrorSourceRuntime,
      velox::error_code::kMemCapExceeded,
      {.code = 0x00020007,
       .name = "EXCEEDED_LOCAL_MEMORY_LIMIT",
       .type = protocol::ErrorType::INSUFFICIENT_RESOURCES});

  registerError(
      velox::error_source::kErrorSourceRuntime,
      velox::error_code::kMemAborted,
      {.code = 0x00020000,
       .name = "GENERIC_INSUFFICIENT_RESOURCES",
       .type = protocol::ErrorType::INSUFFICIENT_RESOURCES});

  registerError(
      velox::error_source::kErrorSourceRuntime,
      velox::error_code::kSpillLimitExceeded,
      {.code = 0x00020006,
       .name = "EXCEEDED_SPILL_LIMIT",
       .type = protocol::ErrorType::INSUFFICIENT_RESOURCES});

  registerError(
      velox::error_source::kErrorSourceRuntime,
      velox::error_code::kMemArbitrationFailure,
      {.code = 0x00020000,
       .name = "MEMORY_ARBITRATION_FAILURE",
       .type = protocol::ErrorType::INSUFFICIENT_RESOURCES});

  registerError(
      velox::error_source::kErrorSourceRuntime,
      velox::error_code::kMemArbitrationTimeout,
      {.code = 0x00020000,
       .name = "GENERIC_INSUFFICIENT_RESOURCES",
       .type = protocol::ErrorType::INSUFFICIENT_RESOURCES});

  registerError(
      velox::error_source::kErrorSourceRuntime,
      velox::error_code::kMemAllocError,
      {.code = 0x00020000,
       .name = "GENERIC_INSUFFICIENT_RESOURCES",
       .type = protocol::ErrorType::INSUFFICIENT_RESOURCES});

  registerError(
      velox::error_source::kErrorSourceRuntime,
      velox::error_code::kInvalidState,
      {.code = 0x00010000,
       .name = "GENERIC_INTERNAL_ERROR",
       .type = protocol::ErrorType::INTERNAL_ERROR});

  registerError(
      velox::error_source::kErrorSourceRuntime,
      velox::error_code::kGenericSpillFailure,
      {.code = 0x00010023,
       .name = "GENERIC_SPILL_FAILURE",
       .type = protocol::ErrorType::INTERNAL_ERROR});

  registerError(
      velox::error_source::kErrorSourceRuntime,
      velox::error_code::kUnreachableCode,
      {.code = 0x00010000,
       .name = "GENERIC_INTERNAL_ERROR",
       .type = protocol::ErrorType::INTERNAL_ERROR});

  registerError(
      velox::error_source::kErrorSourceRuntime,
      velox::error_code::kNotImplemented,
      {.code = 0x00010000,
       .name = "GENERIC_INTERNAL_ERROR",
       .type = protocol::ErrorType::INTERNAL_ERROR});

  registerError(
      velox::error_source::kErrorSourceRuntime,
      velox::error_code::kUnknown,
      {.code = 0x00010000,
       .name = "GENERIC_INTERNAL_ERROR",
       .type = protocol::ErrorType::INTERNAL_ERROR});

  registerError(
      velox::error_source::kErrorSourceRuntime,
      presto::error_code::kExceededLocalBroadcastJoinMemoryLimit,
      {.code = 0x0002000C,
       .name = "EXCEEDED_LOCAL_BROADCAST_JOIN_MEMORY_LIMIT",
       .type = protocol::ErrorType::INSUFFICIENT_RESOURCES});

  // Register user errors
  registerError(
      velox::error_source::kErrorSourceUser,
      velox::error_code::kInvalidArgument,
      {.code = 0x00000000,
       .name = "GENERIC_USER_ERROR",
       .type = protocol::ErrorType::USER_ERROR});

  registerError(
      velox::error_source::kErrorSourceUser,
      velox::error_code::kUnsupported,
      {.code = 0x0000000D,
       .name = "NOT_SUPPORTED",
       .type = protocol::ErrorType::USER_ERROR});

  registerError(
      velox::error_source::kErrorSourceUser,
      velox::error_code::kUnsupportedInputUncatchable,
      {.code = 0x0000000D,
       .name = "NOT_SUPPORTED",
       .type = protocol::ErrorType::USER_ERROR});

  registerError(
      velox::error_source::kErrorSourceUser,
      velox::error_code::kArithmeticError,
      {.code = 0x00000000,
       .name = "GENERIC_USER_ERROR",
       .type = protocol::ErrorType::USER_ERROR});

  registerError(
      velox::error_source::kErrorSourceUser,
      velox::error_code::kSchemaMismatch,
      {.code = 0x00000000,
       .name = "GENERIC_USER_ERROR",
       .type = protocol::ErrorType::USER_ERROR});
}

void VeloxToPrestoExceptionTranslator::registerError(
    const std::string& errorSource,
    const std::string& errorCode,
    const protocol::ErrorCode& prestoErrorCode) {
  auto& innerMap = errorMap_[errorSource];
  auto [it, inserted] = innerMap.emplace(errorCode, prestoErrorCode);
  VELOX_CHECK(
      inserted,
      "Duplicate errorCode '{}' for errorSource '{}' is not allowed. "
      "Existing mapping: [code={}, name={}, type={}]",
      errorCode,
      errorSource,
      it->second.code,
      it->second.name,
      static_cast<int>(it->second.type));
}

protocol::ExecutionFailureInfo VeloxToPrestoExceptionTranslator::translate(
    const velox::VeloxException& e) const {
  protocol::ExecutionFailureInfo error;
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
  if (e.stackTrace()) {
    error.stack = e.stackTrace()->toStrVector();
  }

  const auto& errorSource = e.errorSource();
  const auto& errorCode = e.errorCode();

  auto itrErrorCodesMap = errorMap_.find(errorSource);
  if (itrErrorCodesMap != errorMap_.end()) {
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
    const std::exception& e) const {
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

protocol::NativeSidecarFailureInfo toNativeSidecarFailureInfo(
    const protocol::ExecutionFailureInfo& failure) {
  facebook::presto::protocol::NativeSidecarFailureInfo nativeSidecarFailureInfo;
  nativeSidecarFailureInfo.type = failure.type;
  nativeSidecarFailureInfo.message = failure.message;
  nativeSidecarFailureInfo.stack = failure.stack;
  nativeSidecarFailureInfo.errorCode = failure.errorCode;
  return nativeSidecarFailureInfo;
}
} // namespace facebook::presto
