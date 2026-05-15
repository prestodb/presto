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
#include "velox/common/base/Exceptions.h"

using namespace facebook;
using namespace facebook::velox;
using namespace facebook::presto;

namespace {
folly::Singleton<facebook::presto::VeloxToPrestoExceptionTranslator>
    exceptionTranslatorSingleton([]() {
      return new facebook::presto::VeloxToPrestoExceptionTranslator();
    });
} // namespace

TEST(VeloxToPrestoExceptionTranslatorTest, exceptionTranslation) {
  FLAGS_velox_exception_user_stacktrace_enabled = true;
  for (const bool withContext : {false, true}) {
    for (const bool withAdditionalContext : {false, true}) {
      SCOPED_TRACE(fmt::format("withContext: {}", withContext));
      // Setup context based on 'withContext' flag.
      auto contextMessageFunction = [](VeloxException::Type type, auto* arg) {
        return std::string(static_cast<char*>(arg));
      };
      std::string additonalMessage = "additional context message";
      facebook::velox::ExceptionContextSetter additionalContextSetter(
          withAdditionalContext
              ? ExceptionContext{contextMessageFunction, additonalMessage.data(), true}
              : ExceptionContext{});

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
          velox::error_code::kArithmeticError,
          false);

      EXPECT_THROW({ throw userException; }, VeloxException);
      try {
        throw userException;
      } catch (const VeloxException& e) {
        EXPECT_EQ(e.exceptionName(), "VeloxUserError");
        EXPECT_EQ(e.errorSource(), error_source::kErrorSourceUser);
        EXPECT_EQ(e.errorCode(), velox::error_code::kArithmeticError);

        auto translator =
            folly::Singleton<VeloxToPrestoExceptionTranslator>::try_get();
        ASSERT_NE(translator, nullptr);
        auto failureInfo = translateToPrestoException(e);
        EXPECT_EQ(failureInfo.type, e.exceptionName());
        EXPECT_EQ(failureInfo.errorLocation.lineNumber, e.line());
        EXPECT_EQ(failureInfo.errorCode.name, "GENERIC_USER_ERROR");
        EXPECT_EQ(failureInfo.errorCode.code, 0x00000000);
        std::string expectedMessage = "operator() test message";
        if (withContext) {
          expectedMessage += " " + contextMessage;
        }
        if (withAdditionalContext) {
          expectedMessage += " " + additonalMessage;
        }
        EXPECT_EQ(failureInfo.message, expectedMessage);
        EXPECT_EQ(failureInfo.errorCode.type, protocol::ErrorType::USER_ERROR);
      }
    }
  }

  VeloxRuntimeError runtimeException(
      "file_name",
      1,
      "function_name()",
      "operator()",
      "test message",
      "",
      velox::error_code::kInvalidState,
      false);

  EXPECT_THROW({ throw runtimeException; }, VeloxException);
  try {
    throw runtimeException;
  } catch (const VeloxException& e) {
    EXPECT_EQ(e.exceptionName(), "VeloxRuntimeError");
    EXPECT_EQ(e.errorSource(), error_source::kErrorSourceRuntime);
    EXPECT_EQ(e.errorCode(), velox::error_code::kInvalidState);

    auto translator =
        folly::Singleton<VeloxToPrestoExceptionTranslator>::try_get();
    ASSERT_NE(translator, nullptr);
    auto failureInfo = translateToPrestoException(e);
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
    EXPECT_EQ(e.errorCode(), velox::error_code::kInvalidArgument);

    auto translator =
        folly::Singleton<VeloxToPrestoExceptionTranslator>::try_get();
    ASSERT_NE(translator, nullptr);
    auto failureInfo = translateToPrestoException(e);
    EXPECT_EQ(failureInfo.type, e.exceptionName());
    EXPECT_EQ(failureInfo.errorLocation.lineNumber, e.line());
    EXPECT_EQ(failureInfo.errorCode.name, "GENERIC_USER_ERROR");
    EXPECT_EQ(failureInfo.errorCode.code, 0x00000000);
    EXPECT_EQ(failureInfo.errorCode.type, protocol::ErrorType::USER_ERROR);
    EXPECT_EQ(failureInfo.message, "1 == 2 (1 vs. 2) test user error message");
    EXPECT_NE(failureInfo.stack.size(), 0);
  }

  std::runtime_error stdRuntimeError("Test error message");
  auto translator =
      folly::Singleton<VeloxToPrestoExceptionTranslator>::try_get();
  ASSERT_NE(translator, nullptr);
  auto failureInfo = translateToPrestoException((stdRuntimeError));
  EXPECT_EQ(failureInfo.type, "std::exception");
  EXPECT_EQ(failureInfo.errorLocation.lineNumber, 1);
  EXPECT_EQ(failureInfo.errorCode.name, "GENERIC_INTERNAL_ERROR");
  EXPECT_EQ(failureInfo.errorCode.code, 0x00010000);
  EXPECT_EQ(failureInfo.errorCode.type, protocol::ErrorType::INTERNAL_ERROR);
}

TEST(VeloxToPrestoExceptionTranslatorTest, allErrorCodeTranslations) {
  // Test all error codes in the translation map to ensure they translate
  // correctly
  auto translator = folly::Singleton<
      facebook::presto::VeloxToPrestoExceptionTranslator>::try_get();
  ASSERT_NE(translator, nullptr);
  const auto& translateMap = translator->testingErrorMap();

  for (const auto& [errorSource, errorCodeMap] : translateMap) {
    for (const auto& [errorCode, expectedErrorCode] : errorCodeMap) {
      SCOPED_TRACE(
          fmt::format(
              "errorSource: {}, errorCode: {}", errorSource, errorCode));

      // Determine the exception type based on error source
      if (errorSource == velox::error_source::kErrorSourceRuntime) {
        VeloxRuntimeError runtimeException(
            "test_file.cpp",
            42,
            "testFunction()",
            "testExpression",
            "test error message",
            "",
            errorCode,
            false);

        auto failureInfo = translator->translate(runtimeException);

        EXPECT_EQ(failureInfo.errorCode.code, expectedErrorCode.code)
            << "Error code mismatch for " << errorCode;
        EXPECT_EQ(failureInfo.errorCode.name, expectedErrorCode.name)
            << "Error name mismatch for " << errorCode;
        EXPECT_EQ(failureInfo.errorCode.type, expectedErrorCode.type)
            << "Error type mismatch for " << errorCode;
        EXPECT_EQ(failureInfo.type, "VeloxRuntimeError");
        EXPECT_EQ(failureInfo.errorLocation.lineNumber, 42);

      } else if (errorSource == velox::error_source::kErrorSourceUser) {
        VeloxUserError userException(
            "test_file.cpp",
            42,
            "testFunction()",
            "testExpression",
            "test error message",
            "",
            errorCode,
            false);

        auto failureInfo = translator->translate(userException);

        EXPECT_EQ(failureInfo.errorCode.code, expectedErrorCode.code)
            << "Error code mismatch for " << errorCode;
        EXPECT_EQ(failureInfo.errorCode.name, expectedErrorCode.name)
            << "Error name mismatch for " << errorCode;
        EXPECT_EQ(failureInfo.errorCode.type, expectedErrorCode.type)
            << "Error type mismatch for " << errorCode;
        EXPECT_EQ(failureInfo.type, "VeloxUserError");
        EXPECT_EQ(failureInfo.errorLocation.lineNumber, 42);

      } else if (errorSource == velox::error_source::kErrorSourceSystem) {
        FAIL();
      }
    }
  }
}

TEST(UtilsTest, general) {
  EXPECT_EQ("2021-05-20T19:18:27.001Z", util::toISOTimestamp(1621538307001l));
  EXPECT_EQ("2021-05-20T19:18:27.000Z", util::toISOTimestamp(1621538307000l));
}

TEST(UtilsTest, extractMessageBody) {
  std::vector<std::unique_ptr<folly::IOBuf>> body;
  body.push_back(folly::IOBuf::copyBuffer("body1"));
  body.push_back(folly::IOBuf::copyBuffer("body2"));
  body.push_back(folly::IOBuf::copyBuffer("body3"));
  auto iobuf = folly::IOBuf::copyBuffer("body4");
  iobuf->appendToChain(folly::IOBuf::copyBuffer("body5"));
  body.push_back(std::move(iobuf));
  auto messageBody = util::extractMessageBody(body);
  EXPECT_EQ(messageBody, "body1body2body3body4body5");
}

TEST(UtilsTest, getFunctionNameParts) {
  {
    auto parts = util::getFunctionNameParts("presto.default.my_function");
    ASSERT_EQ(parts.size(), 3);
    EXPECT_EQ(parts[0], "presto");
    EXPECT_EQ(parts[1], "default");
    EXPECT_EQ(parts[2], "my_function");
  }

  {
    auto parts = util::getFunctionNameParts("remote.catalog.sum");
    ASSERT_EQ(parts.size(), 3);
    EXPECT_EQ(parts[0], "remote");
    EXPECT_EQ(parts[1], "catalog");
    EXPECT_EQ(parts[2], "sum");
  }
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::SingletonVault::singleton()->registrationComplete();
  return RUN_ALL_TESTS();
}
