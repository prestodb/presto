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
#include "presto_cpp/main/MemorySampling.h"

#include "presto_cpp/main/PeriodicMemoryChecker.h"
#include "presto_cpp/main/common/Counters.h"
#include "velox/common/base/StatsReporter.h"

namespace facebook::presto {

std::shared_ptr<int64_t> sampleSystemMemoryAndRecord(
    PeriodicMemoryChecker* checker) {
  if (checker == nullptr) {
    return nullptr;
  }
  const int64_t bytes = checker->systemUsedMemoryBytes();
  RECORD_METRIC_VALUE(kCounterMemoryCheckerUsedBytes, bytes);
  return std::make_shared<int64_t>(bytes);
}

} // namespace facebook::presto
