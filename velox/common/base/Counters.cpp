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

#include "velox/common/base/Counters.h"
#include "velox/common/base/StatsReporter.h"

namespace facebook::velox {

void registerVeloxCounters() {
  // Track hive handle generation latency in range of [0, 100s] and reports
  // P50, P90, P99, and P100.
  REPORT_ADD_HISTOGRAM_EXPORT_PERCENTILE(
      kCounterHiveFileHandleGenerateLatencyMs, 10, 0, 100000, 50, 90, 99, 100);
}

} // namespace facebook::velox
