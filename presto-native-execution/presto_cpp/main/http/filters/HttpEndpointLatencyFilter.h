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

#pragma once

#include <proxygen/httpserver/Filters.h>
#include <proxygen/httpserver/RequestHandlerFactory.h>
#include "presto_cpp/main/http/HttpServer.h"
#include "velox/common/base/SuccinctPrinter.h"
#include "velox/common/time/Timer.h"

namespace facebook::presto::http::filters {

class HttpEndpointLatencyFilter : public proxygen::Filter {
 public:
  struct EndPointMetrics {
    /// The endpoint of interest, with method and path information in it. e.g.
    /// "GET /v1/task"
    std::string endpoint;

    /// Maximum latency of the request on this endpoint
    uint64_t maxLatencyUs;

    /// Average latency of the request on this endpoint
    uint64_t avgLatencyUs;

    /// Number of requests on this endpoint
    uint64_t count;

    std::string toString() const {
      std::stringstream oss;
      oss << "{'" << endpoint << "' : " << velox::succinctMicros(maxLatencyUs)
          << "(max) " << velox::succinctMicros(avgLatencyUs) << "(avg) "
          << count << "(count)}";
      return oss.str();
    }
  };

  HttpEndpointLatencyFilter(
      proxygen::RequestHandler* upstream,
      const std::shared_ptr<std::unordered_map<
          proxygen::HTTPMethod,
          std::vector<std::unique_ptr<EndPoint>>>>& endpoints);

  static std::vector<EndPointMetrics> retrieveLatencies();

  void onRequest(std::unique_ptr<proxygen::HTTPMessage> msg) noexcept override;

  void requestComplete() noexcept override;

  void onError(proxygen::ProxygenError err) noexcept override;

 private:
  static void updateLatency(const std::string& endpoint, uint64_t latencyMs);

  static folly::Synchronized<std::unordered_map<std::string, EndPointMetrics>>&
  metricMap() {
    static folly::Synchronized<std::unordered_map<std::string, EndPointMetrics>>
        metricMap_;
    return metricMap_;
  }

  // The endpoints the server is listening on. This is used to match the current
  // request's endpoint.
  const std::shared_ptr<std::unordered_map<
      proxygen::HTTPMethod,
      std::vector<std::unique_ptr<EndPoint>>>>
      endpoints_;

  // The http endpoint of this request
  std::string requestEndpoint_;

  // The timer used for keeping track of the duration of the request.
  std::unique_ptr<velox::MicrosecondTimer> timer_;

  // The duration in us this request takes.
  uint64_t timeUs_{0};
};

class HttpEndpointLatencyFilterFactory
    : public proxygen::RequestHandlerFactory {
 public:
  explicit HttpEndpointLatencyFilterFactory(http::HttpServer* httpServer)
      : endpoints_(std::make_shared<std::unordered_map<
                       proxygen::HTTPMethod,
                       std::vector<std::unique_ptr<EndPoint>>>>(
            httpServer->endpoints())) {}

  void onServerStart(folly::EventBase* /*evb*/) noexcept override {}

  void onServerStop() noexcept override {}

  proxygen::RequestHandler* onRequest(
      proxygen::RequestHandler* handler,
      proxygen::HTTPMessage*) noexcept override {
    return new HttpEndpointLatencyFilter(handler, endpoints_);
  }

 private:
  const std::shared_ptr<std::unordered_map<
      proxygen::HTTPMethod,
      std::vector<std::unique_ptr<EndPoint>>>>
      endpoints_;
};

} // namespace facebook::presto::http::filters
