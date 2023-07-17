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

#include "presto_cpp/main/http/filters/StatsFilter.h"
#include "presto_cpp/main/common/Counters.h"
#include "velox/common/base/StatsReporter.h"

namespace facebook::presto::http::filters {

StatsFilter::StatsFilter(proxygen::RequestHandler* upstream)
    : Filter(upstream) {}

void StatsFilter::onRequest(
    std::unique_ptr<proxygen::HTTPMessage> msg) noexcept {
  startTime_ = std::chrono::steady_clock::now();
  REPORT_ADD_STAT_VALUE(kCounterNumHTTPRequest, 1);
  Filter::onRequest(std::move(msg));
}

void StatsFilter::requestComplete() noexcept {
  REPORT_ADD_STAT_VALUE(
      kCounterHTTPRequestLatencyMs,
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now() - startTime_)
          .count());
  delete this;
}

void StatsFilter::onError(proxygen::ProxygenError err) noexcept {
  REPORT_ADD_STAT_VALUE(kCounterNumHTTPRequestError, 1);
  delete this;
}

} // namespace facebook::presto::http::filters
