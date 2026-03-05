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
#include "presto_cpp/main/properties/session/CudfSessionProperties.h"
#include "velox/experimental/cudf/common/CudfConfig.h"

#include <boost/algorithm/string.hpp>
#include <fmt/format.h>

#include <glog/logging.h>

namespace facebook::presto::cudf {

using namespace facebook::velox;

CudfSessionProperties* CudfSessionProperties::instance() {
  static std::unique_ptr<CudfSessionProperties> instance =
      std::make_unique<CudfSessionProperties>();
  return instance.get();
}

// Initialize GPU session properties from cuDF configuration
CudfSessionProperties::CudfSessionProperties() {
  using facebook::velox::cudf_velox::CudfQueryConfig;

  auto sanitizeAndAddSessionProperty =
      [this](const CudfQueryConfig::CudfConfigEntry& entry) {
        auto sessionPropertyName = entry.name;
        if (!boost::algorithm::starts_with(sessionPropertyName, "cudf.")) {
          sessionPropertyName = fmt::format("cudf_{}", sessionPropertyName);
        }
        boost::algorithm::replace_all(sessionPropertyName, ".", "_");
        boost::algorithm::replace_all(sessionPropertyName, "-", "_");

        addSessionProperty(
            sessionPropertyName,
            "cuDF configuration property mapped from Velox",
            entry.type,
            false,
            entry.name,
            entry.defaultValue);
      };

  for (const auto& entry : CudfQueryConfig::getInstance().getConfigs()) {
    sanitizeAndAddSessionProperty(entry);
  }
}

} // namespace facebook::presto::cudf
