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
protocol::ExecutionFailureInfo VeloxToPrestoExceptionTranslator::translate(
    const velox::VeloxException& e) {
  protocol::ExecutionFailureInfo error;
  // Line number must be >= 1
  error.errorLocation.lineNumber = e.line() >= 1 ? e.line() : 1;
  error.errorLocation.columnNumber = 1;
  error.type = e.exceptionName();
  error.message = fmt::format("{} {}", e.failingExpression(), e.message());
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
