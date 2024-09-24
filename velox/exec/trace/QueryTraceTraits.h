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

#pragma once

#include <string>

namespace facebook::velox::exec::trace {
/// Defines the shared constants used by query trace implementation.
struct QueryTraceTraits {
  static inline const std::string kPlanNodeKey = "planNode";
  static inline const std::string kQueryConfigKey = "queryConfig";
  static inline const std::string kDataTypeKey = "rowType";
  static inline const std::string kConnectorPropertiesKey =
      "connectorProperties";
  static inline const std::string kTraceLimitExceededKey = "traceLimitExceeded";

  static inline const std::string kQueryMetaFileName = "query_meta.json";
  static inline const std::string kDataSummaryFileName = "data_summary.json";
  static inline const std::string kDataFileName = "trace.data";
};
} // namespace facebook::velox::exec::trace
