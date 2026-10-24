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

#include <cstdint>
#include <memory>

namespace facebook::presto {

class PeriodicMemoryChecker;

/// Reads the current cached system-memory sample from `checker`. When
/// `checker` is non-null, records the sample to the
/// `presto_cpp.memory_checker_used_bytes` metric and returns it in a
/// shared_ptr. When `checker` is null, returns an empty shared_ptr and does
/// not record the metric, so the time series is absent (not zero) and
/// scrapers can tell "not sampled" apart from "sampled, got 0".
///
/// Lives in its own translation unit so a stub `PeriodicMemoryChecker` can
/// drive it in a unit test without constructing a full `PrestoServer`.
std::shared_ptr<int64_t> sampleSystemMemoryAndRecord(
    PeriodicMemoryChecker* checker);

} // namespace facebook::presto
