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
#include "presto_cpp/main/http/tests/HttpTestBase.h"
#include "velox/common/base/StatsReporter.h"

using namespace facebook::presto;
using namespace facebook::velox;
using namespace facebook::velox::memory;

#ifndef PRESTO_STATS_REPORTER_TYPE
// Initialize singleton for the reporter.
folly::Singleton<facebook::velox::BaseStatsReporter> reporter([]() {
  return new facebook::velox::DummyStatsReporter();
});
#else
folly::Singleton<facebook::velox::BaseStatsReporter> reporter(
    []() -> facebook::velox::BaseStatsReporter* {
      return facebook::presto::prometheus::PrometheusStatsReporter::
          createPrometheusReporter()
              .release();
    });
#endif

class PrestoServerAPITests : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  std::unique_ptr<http::HttpResponse> produceMetricsResponse(
      const bool useHttps,
      const std::string& mimeType) {
    auto memoryPool = memory::MemoryManager::getInstance()->addLeafPool(
        "PrestoServerAPITests");
    auto ioPool = std::make_shared<folly::IOThreadPoolExecutor>(
        8, std::make_shared<folly::NamedThreadFactory>("HTTPSrvIO"));
    auto server = getHttpServer(useHttps, ioPool);

    server->registerGet(
        "/v1/info/metrics",
        [](proxygen::HTTPMessage* message,
           const std::vector<std::unique_ptr<folly::IOBuf>>& /*body*/,
           proxygen::ResponseHandler* downstream) {
          auto acceptHeader = message->getHeaders().getSingleOrEmpty(
              proxygen::HTTPHeaderCode::HTTP_HEADER_ACCEPT);
          if (acceptHeader.find(http::kMimeTypeTextPlain) !=
              std::string::npos) {
            http::sendOkTextResponse(
                downstream,
                folly::Singleton<facebook::velox::BaseStatsReporter>::try_get()
                    ->fetchMetrics());
          } else {
            http::sendOkResponse(
                downstream,
                folly::Singleton<facebook::velox::BaseStatsReporter>::try_get()
                    ->fetchMetrics());
          }
        });

    HttpServerWrapper wrapper(std::move(server));
    auto serverAddress = wrapper.start().get();
    HttpClientFactory clientFactory;
    // Make sure client doesn't timeout for a delayed request.
    auto client = clientFactory.newClient(
        serverAddress,
        std::chrono::milliseconds(1'000),
        std::chrono::milliseconds(0),
        useHttps,
        memoryPool);
    auto responseFuture =
        facebook::presto::http::RequestBuilder()
            .method(proxygen::HTTPMethod::GET)
            .header(proxygen::HTTPHeaderCode::HTTP_HEADER_ACCEPT, mimeType)
            .url("/v1/info/metrics")
            .send(client.get(), "");
    auto response = std::move(responseFuture).get();
    wrapper.stop();
    return response;
  }
};

TEST_F(PrestoServerAPITests, metricsAPIContentTypeText) {
  auto response =
      std::move(produceMetricsResponse(false, http::kMimeTypeTextPlain));
  EXPECT_EQ(response->headers()->getStatusCode(), http::kHttpOk);
  EXPECT_EQ(
      response->headers()->getHeaders().getSingleOrEmpty(
          proxygen::HTTPHeaderCode::HTTP_HEADER_CONTENT_TYPE),
      http::kMimeTypeTextPlain);
}

TEST_F(PrestoServerAPITests, metricsAPIContentTypeJson) {
  auto response =
      std::move(produceMetricsResponse(false, http::kMimeTypeApplicationJson));
  EXPECT_EQ(response->headers()->getStatusCode(), http::kHttpOk);
  EXPECT_EQ(
      response->headers()->getHeaders().getSingleOrEmpty(
          proxygen::HTTPHeaderCode::HTTP_HEADER_CONTENT_TYPE),
      http::kMimeTypeApplicationJson);
}
