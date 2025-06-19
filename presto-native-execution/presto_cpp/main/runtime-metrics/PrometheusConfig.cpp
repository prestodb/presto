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

#include "presto_cpp/main/runtime-metrics/PrometheusConfig.h"

namespace facebook::presto::prometheus {

namespace {
// folly::to<> does not generate 'true' and 'false', so we do it ourselves.
std::string bool2String(bool value) {
  return value ? "true" : "false";
}

#define BOOL_PROP(_key_, _val_) {std::string(_key_), bool2String(_val_)}
} // namespace

PrometheusConfig::PrometheusConfig() {
  registeredProps_ =
      std::unordered_map<std::string, folly::Optional<std::string>>{
          BOOL_PROP(kEnablePrometheusHistogramMetricsCollection, false),
      };
}

PrometheusConfig* PrometheusConfig::instance() {
  static std::unique_ptr<PrometheusConfig> instance =
      std::make_unique<PrometheusConfig>();
  return instance.get();
}

bool PrometheusConfig::enablePrometheusHistogramMetricsCollection() const {
  return optionalProperty<bool>(kEnablePrometheusHistogramMetricsCollection)
      .value();
}
} // namespace facebook::presto::prometheus
