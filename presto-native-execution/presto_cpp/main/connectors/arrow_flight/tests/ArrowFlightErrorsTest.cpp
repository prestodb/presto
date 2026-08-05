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
#include "presto_cpp/main/connectors/arrow_flight/ArrowFlightErrors.h"
#include <arrow/flight/api.h>
#include <arrow/status.h>
#include <gtest/gtest.h>
#include "presto_cpp/main/common/Exception.h"
#include "velox/common/base/Exceptions.h"

using namespace arrow;

namespace facebook::presto::test {

TEST(ArrowFlightErrorsTest, errorCategories) {
  EXPECT_EQ(
      errorCategory(flight::MakeFlightError(
          flight::FlightStatusCode::Unavailable, "unavailable")),
      "external");
  EXPECT_EQ(
      errorCategory(flight::MakeFlightError(
          flight::FlightStatusCode::Unauthenticated, "unauthenticated")),
      "external");
  EXPECT_EQ(
      errorCategory(arrow::Status::CapacityError("capacity")),
      "insufficient_resources");
  EXPECT_EQ(errorCategory(arrow::Status::IOError("io")), "external");
  EXPECT_EQ(
      errorCategory(arrow::Status::Invalid("invalid")), "internal_error");
  EXPECT_EQ(
      errorCategory(arrow::Status::UnknownError("unknown")), "internal_error");
  // Presto error metadata takes precedence over the Flight status code.
  EXPECT_EQ(
      errorCategory(flight::MakeFlightError(
          flight::FlightStatusCode::Unavailable,
          "boom",
          "presto-error-name=NOT_FOUND\npresto-error-type=USER_ERROR")),
      "user_error");
}

// The metric category and the raised error must stay in agreement; they are two
// views of the same failure and are derived independently.
TEST(ArrowFlightErrorsTest, categoryMatchesRaisedError) {
  const struct {
    arrow::Status status;
    std::string_view category;
    std::string_view errorCode;
  } cases[] = {
      {flight::MakeFlightError(flight::FlightStatusCode::Unavailable, "down"),
       "external",
       "ARROW_FLIGHT_UNAVAILABLE_ERROR"},
      {flight::MakeFlightError(
           flight::FlightStatusCode::Unauthenticated, "bad token"),
       "external",
       "ARROW_FLIGHT_AUTH_ERROR"},
      {flight::MakeFlightError(flight::FlightStatusCode::Cancelled, "cancel"),
       "internal_error",
       "ARROW_FLIGHT_INTERNAL_ERROR"},
      {arrow::Status::CapacityError("capacity"),
       "insufficient_resources",
       "ARROW_FLIGHT_RESOURCE_ERROR"},
      {arrow::Status::IOError("io"),
       "external",
       "ARROW_FLIGHT_UNAVAILABLE_ERROR"},
      {arrow::Status::Invalid("invalid"),
       "internal_error",
       "ARROW_FLIGHT_INTERNAL_ERROR"},
  };

  for (const auto& testCase : cases) {
    EXPECT_EQ(errorCategory(testCase.status), testCase.category)
        << testCase.status.ToString();
    try {
      raiseFlightError(testCase.status);
      FAIL() << "expected raiseFlightError to throw";
    } catch (const velox::VeloxException& e) {
      EXPECT_EQ(e.errorCode(), testCase.errorCode)
          << testCase.status.ToString();
    }
  }
}

TEST(ArrowFlightErrorsTest, raiseFlightError) {
  auto expectError = [](const arrow::Status& status,
                        std::string_view errorCode,
                        std::string_view errorSource,
                        bool retriable) {
    try {
      raiseFlightError(status);
      FAIL() << "expected raiseFlightError to throw";
    } catch (const velox::VeloxException& e) {
      EXPECT_EQ(e.errorCode(), errorCode);
      EXPECT_EQ(e.errorSource(), errorSource);
      EXPECT_EQ(e.isRetriable(), retriable);
    }
  };

  // Metadata-driven mapping, regardless of the Flight status code.
  expectError(
      flight::MakeFlightError(
          flight::FlightStatusCode::Internal,
          "table dropped",
          "presto-error-name=NOT_FOUND\npresto-error-type=USER_ERROR\n"
          "presto-error-retriable=false"),
      "INVALID_ARGUMENT",
      "USER",
      false);
  expectError(
      flight::MakeFlightError(
          flight::FlightStatusCode::Unavailable,
          "connection refused",
          "presto-error-name=JDBC_ERROR\npresto-error-type=EXTERNAL\n"
          "presto-error-retriable=true"),
      "ARROW_FLIGHT_REMOTE_ERROR",
      "EXTERNAL",
      true);
  expectError(
      flight::MakeFlightError(
          flight::FlightStatusCode::Internal,
          "oom",
          "presto-error-name=GENERIC_INSUFFICIENT_RESOURCES\n"
          "presto-error-type=INSUFFICIENT_RESOURCES"),
      "ARROW_FLIGHT_RESOURCE_ERROR",
      "RUNTIME",
      false);
  expectError(
      flight::MakeFlightError(
          flight::FlightStatusCode::Internal,
          "npe",
          "presto-error-name=GENERIC_INTERNAL_ERROR\n"
          "presto-error-type=INTERNAL_ERROR"),
      "ARROW_FLIGHT_INTERNAL_ERROR",
      "RUNTIME",
      false);

  // Fallback mapping when no metadata is present.
  expectError(
      flight::MakeFlightError(
          flight::FlightStatusCode::Unavailable, "server down"),
      "ARROW_FLIGHT_UNAVAILABLE_ERROR",
      "EXTERNAL",
      true);
  expectError(
      flight::MakeFlightError(
          flight::FlightStatusCode::Unauthenticated, "bad token"),
      "ARROW_FLIGHT_AUTH_ERROR",
      "EXTERNAL",
      false);
  expectError(
      arrow::Status::UnknownError("unknown"),
      "ARROW_FLIGHT_INTERNAL_ERROR",
      "RUNTIME",
      false);

  try {
    raiseFlightError(flight::MakeFlightError(
        flight::FlightStatusCode::Internal,
        "table dropped",
        "presto-error-name=NOT_FOUND\npresto-error-type=USER_ERROR"));
    FAIL() << "expected raiseFlightError to throw";
  } catch (const velox::VeloxException& e) {
    EXPECT_NE(e.message().find("NOT_FOUND: table dropped"), std::string::npos);
  }

  // A complete tuple (including the numeric code) is passed through verbatim.
  try {
    raiseFlightError(flight::MakeFlightError(
        flight::FlightStatusCode::Unavailable,
        "connection refused",
        "presto-error-name=JDBC_ERROR\npresto-error-code=67108864\n"
        "presto-error-type=EXTERNAL\npresto-error-retriable=false"));
    FAIL() << "expected raiseFlightError to throw";
  } catch (const velox::VeloxException& e) {
    EXPECT_EQ(e.errorSource(), "EXTERNAL");
    const auto decoded = passthrough_error::decode(e.errorCode());
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->name, "JDBC_ERROR");
    EXPECT_EQ(decoded->code, 67108864);
    EXPECT_EQ(decoded->type, protocol::ErrorType::EXTERNAL);
    EXPECT_FALSE(decoded->retriable);
  }
}

} // namespace facebook::presto::test
