/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
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

#include <fmt/format.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "velox/common/base/Exceptions.h"

struct Counter {
  mutable int counter = 0;
};

std::ostream& operator<<(std::ostream& os, const Counter& c) {
  os << c.counter;
  ++c.counter;
  return os;
}

template <>
struct fmt::formatter<Counter> {
  constexpr auto parse(format_parse_context& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const Counter& c, FormatContext& ctx) {
    auto x = c.counter++;
    return format_to(ctx.out(), "{}", x);
  }
};

template <typename T>
void verifyException(
    std::function<void()> f,
    std::function<void(const T&)> exceptionVerifier) {
  try {
    f();
    FAIL() << "Expected exception of type " << typeid(T).name()
           << ", but no exception was thrown.";
  } catch (const T& e) {
    exceptionVerifier(e);
  } catch (...) {
    FAIL() << "Expected exception of type " << typeid(T).name()
           << ", but instead got an exception of a different type.";
  }
}

void verifyVeloxException(
    std::function<void()> f,
    const std::string& messagePrefix) {
  verifyException<::facebook::velox::VeloxException>(
      f, [&messagePrefix](const auto& e) {
        EXPECT_TRUE(folly::StringPiece{e.what()}.startsWith(messagePrefix))
            << "\nException message prefix mismatch.\n\nExpected prefix: "
            << messagePrefix << "\n\nActual message: " << e.what();
      });
}

// Ensures that expressions on the stream are not evaluated unless the condition
// is met.
TEST(ExceptionTests, LazyStreamEvaluation) {
  Counter c;

  EXPECT_EQ(0, c.counter);
  VELOX_CHECK(true, "{}", c);
  EXPECT_EQ(0, c.counter);

  EXPECT_THROW(
      ([&]() { VELOX_CHECK(false, "{}", c); })(),
      ::facebook::velox::VeloxRuntimeError);
  EXPECT_EQ(1, c.counter);

  VELOX_CHECK(true, "{}", c);
  EXPECT_EQ(1, c.counter);

  EXPECT_THROW(
      ([&]() { VELOX_USER_CHECK(false, "{}", c); })(),
      ::facebook::velox::VeloxUserError);
  EXPECT_EQ(2, c.counter);

  EXPECT_THROW(
      ([&]() { VELOX_CHECK(false, "{}", c); })(),
      ::facebook::velox::VeloxRuntimeError);
  EXPECT_EQ(3, c.counter);

  // Simple types.
  size_t i = 0;
  VELOX_CHECK(true, "{}", i++);
  EXPECT_EQ(0, i);
  VELOX_CHECK(true, "{}", ++i);
  EXPECT_EQ(0, i);

  EXPECT_THROW(
      ([&]() { VELOX_CHECK(false, "{}", i++); })(),
      ::facebook::velox::VeloxRuntimeError);
  EXPECT_EQ(1, i);
  EXPECT_THROW(
      ([&]() { VELOX_CHECK(false, "{}", ++i); })(),
      ::facebook::velox::VeloxRuntimeError);
  EXPECT_EQ(2, i);
}

TEST(ExceptionTests, ExceptionMessageCheck) {
  verifyVeloxException(
      []() { VELOX_CHECK(4 > 5, "Test message 1"); },
      "Exception: VeloxRuntimeError\nError Source: RUNTIME\n"
      "Error Code: INVALID_STATE\nReason: Test message 1\n"
      "Retriable: False\nExpression: 4 > 5\nFunction: operator()\nFile: ");
}

TEST(ExceptionTests, ExceptionMessageUnreachable) {
  verifyVeloxException(
      []() { VELOX_UNREACHABLE("Test message 3"); },
      "Exception: VeloxRuntimeError\nError Source: RUNTIME\n"
      "Error Code: UNREACHABLE_CODE\nReason: Test message 3\n"
      "Retriable: False\nFunction: operator()\nFile: ");
}

#define RUN_TEST(test)                                                      \
  TEST_##test(                                                              \
      VELOX_CHECK_##test, "RUNTIME", "INVALID_STATE", "VeloxRuntimeError"); \
  TEST_##test(                                                              \
      VELOX_USER_CHECK_##test, "USER", "INVALID_ARGUMENT", "VeloxUserError");

#define TEST_GT(macro, system, code, prefix)                               \
  verifyVeloxException(                                                    \
      []() { macro(4, 5); },                                               \
      "Exception: " prefix "\nError Source: " system "\nError Code: " code \
      "\nReason: (4 vs. 5)"                                                \
      "\nRetriable: False"                                                 \
      "\nExpression: 4 > 5"                                                \
      "\nFunction: operator()"                                             \
      "\nFile: ");                                                         \
                                                                           \
  verifyVeloxException(                                                    \
      []() { macro(3, 3); },                                               \
      "Exception: " prefix "\nError Source: " system "\nError Code: " code \
      "\nReason: (3 vs. 3)"                                                \
      "\nRetriable: False"                                                 \
      "\nExpression: 3 > 3"                                                \
      "\nFunction: operator()"                                             \
      "\nFile: ");                                                         \
                                                                           \
  verifyVeloxException(                                                    \
      []() { macro(-1, 1, "Message 1"); },                                 \
      "Exception: " prefix "\nError Source: " system "\nError Code: " code \
      "\nReason: (-1 vs. 1) Message 1"                                     \
      "\nRetriable: False"                                                 \
      "\nExpression: -1 > 1"                                               \
      "\nFunction: operator()"                                             \
      "\nFile: ");                                                         \
                                                                           \
  macro(3, 2);                                                             \
  macro(1, -1, "Message 2");

TEST(ExceptionTests, GreaterThan) {
  RUN_TEST(GT);
}

#define TEST_GE(macro, system, code, prefix)                               \
  verifyVeloxException(                                                    \
      []() { macro(4, 5); },                                               \
      "Exception: " prefix "\nError Source: " system "\nError Code: " code \
      "\nReason: (4 vs. 5)"                                                \
      "\nRetriable: False"                                                 \
      "\nExpression: 4 >= 5"                                               \
      "\nFunction: operator()"                                             \
      "\nFile: ");                                                         \
                                                                           \
  verifyVeloxException(                                                    \
      []() { macro(-1, 1, "Message 1"); },                                 \
      "Exception: " prefix "\nError Source: " system "\nError Code: " code \
      "\nReason: (-1 vs. 1) Message 1"                                     \
      "\nRetriable: False"                                                 \
      "\nExpression: -1 >= 1"                                              \
      "\nFunction: operator()"                                             \
      "\nFile: ");                                                         \
                                                                           \
  macro(3, 2);                                                             \
  macro(3, 3);                                                             \
  macro(1, -1, "Message 2");

TEST(ExceptionTests, GreaterEqual) {
  RUN_TEST(GE);
}

#define TEST_LT(macro, system, code, prefix)                               \
  verifyVeloxException(                                                    \
      []() { macro(5, 4); },                                               \
      "Exception: " prefix "\nError Source: " system "\nError Code: " code \
      "\nReason: (5 vs. 4)"                                                \
      "\nRetriable: False"                                                 \
      "\nExpression: 5 < 4"                                                \
      "\nFunction: operator()"                                             \
      "\nFile: ");                                                         \
                                                                           \
  verifyVeloxException(                                                    \
      []() { macro(2, 2); },                                               \
      "Exception: " prefix "\nError Source: " system "\nError Code: " code \
      "\nReason: (2 vs. 2)"                                                \
      "\nRetriable: False"                                                 \
      "\nExpression: 2 < 2"                                                \
      "\nFunction: operator()"                                             \
      "\nFile: ");                                                         \
                                                                           \
  verifyVeloxException(                                                    \
      []() { macro(1, -1, "Message 1"); },                                 \
      "Exception: " prefix "\nError Source: " system "\nError Code: " code \
      "\nReason: (1 vs. -1) Message 1"                                     \
      "\nRetriable: False"                                                 \
      "\nExpression: 1 < -1"                                               \
      "\nFunction: operator()"                                             \
      "\nFile: ");                                                         \
                                                                           \
  macro(2, 3);                                                             \
  macro(-1, 1, "Message 2");

TEST(ExceptionTests, LessThan) {
  RUN_TEST(LT);
}

#define TEST_LE(macro, system, code, prefix)                               \
  verifyVeloxException(                                                    \
      []() { macro(6, 2); },                                               \
      "Exception: " prefix "\nError Source: " system "\nError Code: " code \
      "\nReason: (6 vs. 2)"                                                \
      "\nRetriable: False"                                                 \
      "\nExpression: 6 <= 2"                                               \
      "\nFunction: operator()"                                             \
      "\nFile: ");                                                         \
                                                                           \
  verifyVeloxException(                                                    \
      []() { macro(3, -3, "Message 1"); },                                 \
      "Exception: " prefix "\nError Source: " system "\nError Code: " code \
      "\nReason: (3 vs. -3) Message 1"                                     \
      "\nRetriable: False"                                                 \
      "\nExpression: 3 <= -3"                                              \
      "\nFunction: operator()"                                             \
      "\nFile: ");                                                         \
                                                                           \
  macro(5, 54);                                                            \
  macro(1, 1);                                                             \
  macro(-3, 3, "Message 2");

TEST(ExceptionTests, LessEqual) {
  RUN_TEST(LE);
}

#define TEST_EQ(macro, system, code, prefix)                                 \
  {                                                                          \
    verifyVeloxException(                                                    \
        []() { macro(1, 2); },                                               \
        "Exception: " prefix "\nError Source: " system "\nError Code: " code \
        "\nReason: (1 vs. 2)"                                                \
        "\nRetriable: False"                                                 \
        "\nExpression: 1 == 2"                                               \
        "\nFunction: operator()"                                             \
        "\nFile: ");                                                         \
                                                                             \
    verifyVeloxException(                                                    \
        []() { macro(2, 1, "Message 1"); },                                  \
        "Exception: " prefix "\nError Source: " system "\nError Code: " code \
        "\nReason: (2 vs. 1) Message 1"                                      \
        "\nRetriable: False"                                                 \
        "\nExpression: 2 == 1"                                               \
        "\nFunction: operator()"                                             \
        "\nFile: ");                                                         \
                                                                             \
    auto t = true;                                                           \
    auto f = false;                                                          \
    macro(521, 521);                                                         \
    macro(1.1, 1.1);                                                         \
    macro(true, t, "Message 2");                                             \
    macro(f, false, "Message 3");                                            \
  }

TEST(ExceptionTests, Equal) {
  RUN_TEST(EQ);
}

#define TEST_NE(macro, system, code, prefix)                                 \
  {                                                                          \
    verifyVeloxException(                                                    \
        []() { macro(1, 1); },                                               \
        "Exception: " prefix "\nError Source: " system "\nError Code: " code \
        "\nReason: (1 vs. 1)"                                                \
        "\nRetriable: False"                                                 \
        "\nExpression: 1 != 1"                                               \
        "\nFunction: operator()"                                             \
        "\nFile: ");                                                         \
                                                                             \
    verifyVeloxException(                                                    \
        []() { macro(2.2, 2.2, "Message 1"); },                              \
        "Exception: " prefix "\nError Source: " system "\nError Code: " code \
        "\nReason: (2.2 vs. 2.2) Message 1"                                  \
        "\nRetriable: False"                                                 \
        "\nExpression: 2.2 != 2.2"                                           \
        "\nFunction: operator()"                                             \
        "\nFile: ");                                                         \
                                                                             \
    auto t = true;                                                           \
    auto f = false;                                                          \
    macro(521, 522);                                                         \
    macro(1.2, 1.1);                                                         \
    macro(true, f, "Message 2");                                             \
    macro(t, false, "Message 3");                                            \
  }

TEST(ExceptionTests, NotEqual) {
  RUN_TEST(NE);
}

#define TEST_NOT_NULL(macro, system, code, prefix)                           \
  {                                                                          \
    verifyVeloxException(                                                    \
        []() { macro(nullptr); },                                            \
        "Exception: " prefix "\nError Source: " system "\nError Code: " code \
        "\nRetriable: False"                                                 \
        "\nExpression: nullptr != nullptr"                                   \
        "\nFunction: operator()"                                             \
        "\nFile: ");                                                         \
    verifyVeloxException(                                                    \
        []() {                                                               \
          std::shared_ptr<int> a;                                            \
          macro(a, "Message 1");                                             \
        },                                                                   \
        "Exception: " prefix "\nError Source: " system "\nError Code: " code \
        "\nReason: Message 1"                                                \
        "\nRetriable: False"                                                 \
        "\nExpression: a != nullptr"                                         \
        "\nFunction: operator()"                                             \
        "\nFile: ");                                                         \
    auto b = std::make_shared<int>(5);                                       \
    macro(b);                                                                \
  }

TEST(ExceptionTests, NotNull) {
  RUN_TEST(NOT_NULL);
}

TEST(ExceptionTests, ExpressionString) {
  size_t i = 1;
  size_t j = 100;
  std::string msgTemplate =
      "Exception: VeloxRuntimeError"
      "\nError Source: RUNTIME"
      "\nError Code: INVALID_STATE"
      "\nReason: ({1})"
      "\nRetriable: False"
      "\nExpression: {0}"
      "\nFunction: operator()"
      "\nFile: ";

  verifyVeloxException(
      [&]() { VELOX_CHECK_EQ(i, j); },
      fmt::format(msgTemplate, "i == j", "1 vs. 100"));

  verifyVeloxException(
      [&]() { VELOX_CHECK_NE(i, 1); },
      fmt::format(msgTemplate, "i != 1", "1 vs. 1"));

  verifyVeloxException(
      [&]() { VELOX_CHECK_LT(i + j, j); },
      fmt::format(msgTemplate, "i + j < j", "101 vs. 100"));

  verifyVeloxException(
      [&]() { VELOX_CHECK_GE(i + j * 2, 1000); },
      fmt::format(msgTemplate, "i + j * 2 >= 1000", "201 vs. 1000"));
}

TEST(ExceptionTests, ExceptionMessageAssertNotImplemented) {
  verifyVeloxException(
      []() { VELOX_NYI(); },
      "Exception: VeloxRuntimeError\nError Source: RUNTIME\n"
      "Error Code: NOT_IMPLEMENTED\n"
      "Retriable: False\nFunction: operator()\nFile: ");

  verifyVeloxException(
      []() { VELOX_NYI("Message 1"); },
      "Exception: VeloxRuntimeError\nError Source: RUNTIME\n"
      "Error Code: NOT_IMPLEMENTED\nReason: Message 1\nRetriable: False\n"
      "Function: operator()\nFile: ");
}

TEST(ExceptionTests, ExceptionWithErrorCode) {
  std::string msgTemplate =
      "Exception: {}"
      "\nError Source: {}"
      "\nError Code: {}"
      "\nRetriable: {}"
      "\nExpression: {}"
      "\nFunction: {}"
      "\nFile: ";

  verifyVeloxException(
      [&]() { VELOX_FAIL(); },
      fmt::format(
          "Exception: {}"
          "\nError Source: {}"
          "\nError Code: {}"
          "\nRetriable: {}"
          "\nFunction: {}"
          "\nFile: ",
          "VeloxRuntimeError",
          "RUNTIME",
          "INVALID_STATE",
          "False",
          "operator()"));

  verifyVeloxException(
      [&]() { VELOX_USER_FAIL(); },
      fmt::format(
          "Exception: {}"
          "\nError Source: {}"
          "\nError Code: {}"
          "\nRetriable: {}"
          "\nFunction: {}"
          "\nFile: ",
          "VeloxUserError",
          "USER",
          "INVALID_ARGUMENT",
          "False",
          "operator()"));
}

TEST(ExceptionTests, context) {
  // No context.
  verifyVeloxException(
      [&]() { VELOX_CHECK_EQ(1, 3); },
      "Exception: VeloxRuntimeError"
      "\nError Source: RUNTIME"
      "\nError Code: INVALID_STATE"
      "\nReason: (1 vs. 3)"
      "\nRetriable: False"
      "\nExpression: 1 == 3"
      "\nFunction: operator()"
      "\nFile: ");

  // With context.
  {
    std::string troubleshootingAid = "Troubleshooting aid.";
    facebook::velox::ExceptionContextSetter context(
        {[](auto* arg) { return std::string(static_cast<char*>(arg)); },
         troubleshootingAid.data()});

    verifyVeloxException(
        [&]() { VELOX_CHECK_EQ(1, 3); },
        "Exception: VeloxRuntimeError"
        "\nError Source: RUNTIME"
        "\nError Code: INVALID_STATE"
        "\nReason: (1 vs. 3)"
        "\nRetriable: False"
        "\nExpression: 1 == 3"
        "\nContext: Troubleshooting aid."
        "\nFunction: operator()"
        "\nFile: ");
  }

  // Different context.
  {
    std::string debuggingInfo = "Debugging info.";
    facebook::velox::ExceptionContextSetter context(
        {[](auto* arg) { return std::string(static_cast<char*>(arg)); },
         debuggingInfo.data()});

    verifyVeloxException(
        [&]() { VELOX_CHECK_EQ(1, 3); },
        "Exception: VeloxRuntimeError"
        "\nError Source: RUNTIME"
        "\nError Code: INVALID_STATE"
        "\nReason: (1 vs. 3)"
        "\nRetriable: False"
        "\nExpression: 1 == 3"
        "\nContext: Debugging info."
        "\nFunction: operator()"
        "\nFile: ");
  }

  // No context.
  verifyVeloxException(
      [&]() { VELOX_CHECK_EQ(1, 3); },
      "Exception: VeloxRuntimeError"
      "\nError Source: RUNTIME"
      "\nError Code: INVALID_STATE"
      "\nReason: (1 vs. 3)"
      "\nRetriable: False"
      "\nExpression: 1 == 3"
      "\nFunction: operator()"
      "\nFile: ");
}
