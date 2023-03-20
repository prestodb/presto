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

#include "presto_cpp/main/http/filters/AccessLogFilter.h"

#include <gtest/gtest.h>
#include <proxygen/httpserver/Mocks.h>
#include <proxygen/lib/http/HTTPMessage.h>

TEST(AccessLogFilterTest, logFormat) {
  class MockedAccessLogFilter
      : public facebook::presto::http::filters::AccessLogFilter {
   public:
    explicit MockedAccessLogFilter(proxygen::RequestHandler* upstream)
        : AccessLogFilter(upstream) {}

    std::function<void(std::string)> verifyFn_;

    void init(std::function<void(std::string)>&& verifyFn) {
      verifyFn_ = verifyFn;
    }

    void writeLog(std::string logLine) const noexcept override {
      verifyFn_(logLine);
    }
  };

  std::unique_ptr<proxygen::HTTPMessage> request =
      std::make_unique<proxygen::HTTPMessage>();
  request->setMethod(proxygen::HTTPMethod::POST);
  request->setURL("/testing/url1");

  std::unique_ptr<proxygen::MockRequestHandler> mock =
      std::make_unique<proxygen::MockRequestHandler>();
  EXPECT_CALL(*mock.get(), requestComplete);

  // Using new here since the base class uses delete in Filters.h:62
  auto requestFilter = new MockedAccessLogFilter(std::move(mock.get()));

  auto verifyFn = [requestFilter](std::string logLine) {
    EXPECT_TRUE(logLine.find("POST") != std::string::npos);
    EXPECT_TRUE(logLine.find("/testing/url1") != std::string::npos);
    EXPECT_TRUE(logLine.find("HTTP") != std::string::npos);
  };
  requestFilter->init(verifyFn);

  requestFilter->onRequest(std::move(request));
  requestFilter->requestComplete();
}
