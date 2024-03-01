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

#include "presto_cpp/main/http/filters/HttpEndpointLatencyFilter.h"
#include "velox/common/base/Exceptions.h"

namespace facebook::presto::http::filters {

HttpEndpointLatencyFilter::HttpEndpointLatencyFilter(
    proxygen::RequestHandler* upstream,
    const std::shared_ptr<std::unordered_map<
        proxygen::HTTPMethod,
        std::vector<std::unique_ptr<EndPoint>>>>& endpoints)
    : Filter(upstream), endpoints_(endpoints) {}

// static
void HttpEndpointLatencyFilter::updateLatency(
    const std::string& endpoint,
    uint64_t latencyUs) {
  metricMap().withWLock([&](std::unordered_map<std::string, EndPointMetrics>&
                                map) {
    auto itr = map.find(endpoint);
    if (itr != map.end()) {
      auto& metrics = itr->second;
      metrics.maxLatencyUs = std::max(metrics.maxLatencyUs, latencyUs);
      metrics.avgLatencyUs =
          (metrics.avgLatencyUs * metrics.count + latencyUs) /
          (metrics.count + 1);
      ++metrics.count;
    } else {
      map.emplace(endpoint, EndPointMetrics{endpoint, latencyUs, latencyUs, 1});
    }
  });
}

// static
std::vector<HttpEndpointLatencyFilter::EndPointMetrics>
HttpEndpointLatencyFilter::retrieveLatencies() {
  std::vector<HttpEndpointLatencyFilter::EndPointMetrics> result;
  metricMap().withWLock([&](std::unordered_map<
                            std::string,
                            HttpEndpointLatencyFilter::EndPointMetrics>& map) {
    result.reserve(map.size());
    for (const auto& pair : map) {
      result.push_back(pair.second);
    }
    map.clear();
  });
  std::sort(result.begin(), result.end(), [](const auto& lhs, const auto& rhs) {
    return lhs.maxLatencyUs > rhs.maxLatencyUs;
  });
  return result;
}

void HttpEndpointLatencyFilter::onRequest(
    std::unique_ptr<proxygen::HTTPMessage> msg) noexcept {
  auto path = msg->getPath();
  const auto method = msg->getMethod().value();
  const auto& endpoints = endpoints_->at(method);

  // Allocate vector outside of loop to avoid repeated alloc/free.
  std::vector<std::string> matches(4);
  std::vector<RE2::Arg> args(4);
  std::vector<RE2::Arg*> argPtrs(4);

  for (const auto& endpoint : endpoints) {
    if (endpoint->check(path, matches, args, argPtrs)) {
      requestEndpoint_ = methodToString(method) + " " + endpoint->pattern();
      break;
    }
  }
  VELOX_CHECK(!requestEndpoint_.empty());

  // Starts the timer.
  timer_ = std::make_unique<velox::MicrosecondTimer>(&timeUs_);
  proxygen::Filter::onRequest(std::move(msg));
}

void HttpEndpointLatencyFilter::requestComplete() noexcept {
  timer_.reset();
  updateLatency(requestEndpoint_, timeUs_);
  proxygen::Filter::requestComplete();
}

void HttpEndpointLatencyFilter::onError(proxygen::ProxygenError err) noexcept {
  timer_.reset();
  updateLatency(requestEndpoint_, timeUs_);
  proxygen::Filter::onError(err);
}

} // namespace facebook::presto::http::filters
