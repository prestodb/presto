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

#include <folly/init/Init.h>
#include <gtest/gtest.h>
#include "presto_cpp/main/http/HttpServer.h"
#include "presto_cpp/main/http/filters/HttpEndpointLatencyFilter.h"

using namespace facebook::presto::http::filters;
using namespace facebook::presto::http;

class HttpFilterTest : public ::testing::Test {};

class DummyRequestHandler : public proxygen::RequestHandler {
 public:
  void onRequest(
      std::unique_ptr<proxygen::HTTPMessage> /*unused*/) noexcept override {}

  void onBody(std::unique_ptr<folly::IOBuf> /*unused*/) noexcept override {}

  void onUpgrade(proxygen::UpgradeProtocol /*unused*/) noexcept override {}

  void requestComplete() noexcept override {}

  void onError(proxygen::ProxygenError /*unused*/) noexcept override {}

  void onEOM() noexcept override {}
};

namespace {
// Builds endpoint map for all methods specified by 'methods'. 'size' specifies
// the number of endpoints for each method.
std::shared_ptr<std::unordered_map<
    proxygen::HTTPMethod,
    std::vector<std::unique_ptr<EndPoint>>>>
buildEndpoints(
    const std::vector<proxygen::HTTPMethod>& methods,
    uint64_t size) {
  std::shared_ptr<std::unordered_map<
      proxygen::HTTPMethod,
      std::vector<std::unique_ptr<EndPoint>>>>
      result = std::make_shared<std::unordered_map<
          proxygen::HTTPMethod,
          std::vector<std::unique_ptr<EndPoint>>>>(methods.size());
  for (const auto& method : methods) {
    (*result)[method] = std::vector<std::unique_ptr<EndPoint>>();
    auto& endpoints = result->at(method);
    for (uint32_t i = 0; i < size; ++i) {
      endpoints.emplace_back(
          std::make_unique<EndPoint>(fmt::format("/v1/ep{}", i), nullptr));
    }
  }
  return result;
}

std::unique_ptr<proxygen::HTTPMessage> buildRequestMsg(
    const proxygen::HTTPMethod& method,
    const std::string& path) {
  auto msg = std::make_unique<proxygen::HTTPMessage>();
  msg->setMethod(method);
  msg->setURL("127.0.0.1:7777" + path);
  return msg;
}
} // namespace

TEST_F(HttpFilterTest, testEndpointLatencyFilter) {
  DummyRequestHandler handler;
  std::vector<proxygen::HTTPMethod> methods{
      proxygen::HTTPMethod::GET,
      proxygen::HTTPMethod::POST,
      proxygen::HTTPMethod::DELETE,
      proxygen::HTTPMethod::HEAD};
  // For 'GET', 'POST', 'DELETE', 'HEAD'
  // /v1/ep0
  // ...
  // /v1/ep4
  const auto endpoints = buildEndpoints(methods, 5);

  std::vector<HttpEndpointLatencyFilter*> filters;
  const auto numMethods = methods.size();
  for (uint32_t i = 0; i < 100; i++) {
    filters.emplace_back(new HttpEndpointLatencyFilter(&handler, endpoints));
    const auto path = "/v1/ep" + std::to_string(i % 5);
    const auto& method = methods[i % numMethods];
    filters.back()->onRequest(buildRequestMsg(method, path));
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  for (uint32_t i = 0; i < filters.size(); i++) {
    auto& filter = filters[i];
    if (i % 2 == 0) {
      filter->requestComplete();
    } else {
      filter->onError(proxygen::ProxygenError());
    }
  }

  auto latencyMetrics = HttpEndpointLatencyFilter::retrieveLatencies();
  ASSERT_EQ(latencyMetrics.size(), 20);
  // After first time retrieval, it should be reset. Second time retrieval
  // should be empty set.
  ASSERT_EQ(HttpEndpointLatencyFilter::retrieveLatencies().size(), 0);

  for (const auto& metrics : latencyMetrics) {
    ASSERT_GT(metrics.maxLatencyUs, 0);
    ASSERT_GT(metrics.avgLatencyUs, 0);
    ASSERT_LE(metrics.avgLatencyUs, metrics.maxLatencyUs);
    ASSERT_GT(metrics.count, 0);
  }
}

TEST_F(HttpFilterTest, testConcurrentEndpointLatencyFilter) {
  DummyRequestHandler handler;
  std::vector<proxygen::HTTPMethod> methods{
      proxygen::HTTPMethod::GET, proxygen::HTTPMethod::POST};
  // GET /v1/ep0
  // POST /v1/ep0
  const auto endpoints = buildEndpoints(methods, 1);

  const uint64_t numRequests = 1000;
  std::vector<HttpEndpointLatencyFilter*> filters;
  filters.reserve(numRequests);
  const auto numMethods = methods.size();
  for (uint32_t i = 0; i < numRequests; i++) {
    filters.emplace_back(new HttpEndpointLatencyFilter(&handler, endpoints));
    const auto path = "/v1/ep0";
    const auto& method = methods[i % numMethods];
    filters.back()->onRequest(buildRequestMsg(method, path));
  }

  std::vector<std::thread> threads;
  threads.reserve(numRequests);
  for (uint32_t i = 0; i < numRequests; i++) {
    threads.emplace_back([&filters, i]() { filters[i]->requestComplete(); });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  auto latencyMetrics = HttpEndpointLatencyFilter::retrieveLatencies();
  ASSERT_EQ(latencyMetrics.size(), 2);
  // After first time retrieval, it should be reset. Second time retrieval
  // should be empty set.
  ASSERT_EQ(HttpEndpointLatencyFilter::retrieveLatencies().size(), 0);

  for (const auto& metrics : latencyMetrics) {
    ASSERT_GT(metrics.maxLatencyUs, 0);
    ASSERT_GT(metrics.avgLatencyUs, 0);
    ASSERT_LE(metrics.avgLatencyUs, metrics.maxLatencyUs);
    ASSERT_EQ(metrics.count, numRequests / 2);
  }
}
