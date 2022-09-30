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
#include <gtest/gtest.h>
#include "presto_cpp/main/common/Exception.h"
#include "presto_cpp/main/common/Utils.h"
#include "presto_cpp/presto_protocol/presto_protocol.h"
#include "velox/common/base/Exceptions.h"

using namespace facebook::velox;
using namespace facebook::presto;

TEST(VeloxToPrestoExceptionTranslatorTest, exceptionTranslation) {
  FLAGS_velox_exception_user_stacktrace_enabled = true;
  for (const bool withContext : {false, true}) {
    SCOPED_TRACE(fmt::format("withContext: {}", withContext));
    // Setup context based on 'withContext' flag.
    auto contextMessageFunction = [](VeloxException::Type type, auto* arg) {
      return std::string(static_cast<char*>(arg));
    };
    std::string contextMessage = "context message";
    facebook::velox::ExceptionContextSetter contextSetter(
        withContext
            ? ExceptionContext{contextMessageFunction, contextMessage.data()}
            : ExceptionContext{});
    VeloxUserError userException(
        "file_name",
        1,
        "function_name()",
        "operator()",
        "test message",
        "",
        error_code::kArithmeticError,
        false);

    EXPECT_THROW({ throw userException; }, VeloxException);
    try {
      throw userException;
    } catch (const VeloxException& e) {
      EXPECT_EQ(e.exceptionName(), "VeloxUserError");
      EXPECT_EQ(e.errorSource(), error_source::kErrorSourceUser);
      EXPECT_EQ(e.errorCode(), error_code::kArithmeticError);

      auto failureInfo = VeloxToPrestoExceptionTranslator::translate(e);
      EXPECT_EQ(failureInfo.type, e.exceptionName());
      EXPECT_EQ(failureInfo.errorLocation.lineNumber, e.line());
      EXPECT_EQ(failureInfo.errorCode.name, "GENERIC_USER_ERROR");
      EXPECT_EQ(failureInfo.errorCode.code, 0x00000000);
      EXPECT_EQ(
          failureInfo.message,
          withContext ? "operator() test message context message"
                      : "operator() test message");
      EXPECT_EQ(failureInfo.errorCode.type, protocol::ErrorType::USER_ERROR);
    }
  }

  VeloxRuntimeError runtimeException(
      "file_name",
      1,
      "function_name()",
      "operator()",
      "test message",
      "",
      error_code::kInvalidState,
      false);

  EXPECT_THROW({ throw runtimeException; }, VeloxException);
  try {
    throw runtimeException;
  } catch (const VeloxException& e) {
    EXPECT_EQ(e.exceptionName(), "VeloxRuntimeError");
    EXPECT_EQ(e.errorSource(), error_source::kErrorSourceRuntime);
    EXPECT_EQ(e.errorCode(), error_code::kInvalidState);

    auto failureInfo = VeloxToPrestoExceptionTranslator::translate(e);
    EXPECT_EQ(failureInfo.type, e.exceptionName());
    EXPECT_EQ(failureInfo.errorLocation.lineNumber, e.line());
    EXPECT_EQ(failureInfo.errorCode.name, "GENERIC_INTERNAL_ERROR");
    EXPECT_EQ(failureInfo.errorCode.code, 0x00010000);
    EXPECT_EQ(failureInfo.errorCode.type, protocol::ErrorType::INTERNAL_ERROR);
  }

  EXPECT_THROW(([]() { VELOX_USER_CHECK_EQ(1, 2); })(), VeloxException);
  try {
    VELOX_USER_CHECK_EQ(1, 2, "test user error message");
  } catch (const VeloxException& e) {
    EXPECT_EQ(e.exceptionName(), "VeloxUserError");
    EXPECT_EQ(e.errorSource(), error_source::kErrorSourceUser);
    EXPECT_EQ(e.errorCode(), error_code::kInvalidArgument);

    auto failureInfo = VeloxToPrestoExceptionTranslator::translate(e);
    EXPECT_EQ(failureInfo.type, e.exceptionName());
    EXPECT_EQ(failureInfo.errorLocation.lineNumber, e.line());
    EXPECT_EQ(failureInfo.errorCode.name, "GENERIC_USER_ERROR");
    EXPECT_EQ(failureInfo.errorCode.code, 0x00000000);
    EXPECT_EQ(failureInfo.errorCode.type, protocol::ErrorType::USER_ERROR);
    EXPECT_EQ(failureInfo.message, "1 == 2 (1 vs. 2) test user error message");
    EXPECT_NE(failureInfo.stack.size(), 0);
  }

  std::runtime_error stdRuntimeError("Test error message");
  auto failureInfo =
      VeloxToPrestoExceptionTranslator::translate((stdRuntimeError));
  EXPECT_EQ(failureInfo.type, "std::exception");
  EXPECT_EQ(failureInfo.errorLocation.lineNumber, 1);
  EXPECT_EQ(failureInfo.errorCode.name, "GENERIC_INTERNAL_ERROR");
  EXPECT_EQ(failureInfo.errorCode.code, 0x00010000);
  EXPECT_EQ(failureInfo.errorCode.type, protocol::ErrorType::INTERNAL_ERROR);
}

TEST(UtilsTest, general) {
  EXPECT_EQ("2021-05-20T19:18:27.001Z", util::toISOTimestamp(1621538307001l));
  EXPECT_EQ("2021-05-20T19:18:27.000Z", util::toISOTimestamp(1621538307000l));
}
