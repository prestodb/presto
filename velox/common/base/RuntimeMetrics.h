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

#include <limits>
#include <sstream>

#include <fmt/format.h>
#include <folly/CppAttributes.h>

namespace facebook::velox {

struct RuntimeCounter {
  enum class Unit { kNone, kNanos, kBytes };
  int64_t value;
  Unit unit{Unit::kNone};

  explicit RuntimeCounter(int64_t _value, Unit _unit = Unit::kNone)
      : value(_value), unit(_unit) {}
};

struct RuntimeMetric {
  // Sum, min, max have the same unit, count has kNone.
  RuntimeCounter::Unit unit;
  int64_t sum{0};
  int64_t count{0};
  int64_t min{std::numeric_limits<int64_t>::max()};
  int64_t max{std::numeric_limits<int64_t>::min()};

  explicit RuntimeMetric(
      RuntimeCounter::Unit _unit = RuntimeCounter::Unit::kNone)
      : unit(_unit) {}

  explicit RuntimeMetric(
      int64_t value,
      RuntimeCounter::Unit _unit = RuntimeCounter::Unit::kNone)
      : unit(_unit), sum{value}, count{1}, min{value}, max{value} {}

  void addValue(int64_t value);

  /// Aggregate sets 'min' and 'max' to 'sum', also sets 'count' to 1 if
  /// positive.
  void aggregate();

  void printMetric(std::stringstream& stream) const;

  void merge(const RuntimeMetric& other);

  std::string toString() const {
    return fmt::format(
        "sum:{}, count:{}, min:{}, max:{}", sum, count, min, max);
  }
};

/// Simple interface to implement writing of runtime stats to Velox Operator
/// stats.
/// Inherit a concrete class from this to implement your writing.
class BaseRuntimeStatWriter {
 public:
  virtual ~BaseRuntimeStatWriter() = default;

  virtual void addRuntimeStat(
      const std::string& /* name */,
      const RuntimeCounter& /* value */) {}
};

/// Setting a concrete runtime stats writer on the thread will ensure that any
/// code can add runtime counters to the current Operator running on that
/// thread.
/// NOTE: This is only used by the Velox Driver at the moment, which ensures the
/// active Operator is being used by the writer.
void setThreadLocalRunTimeStatWriter(
    BaseRuntimeStatWriter* FOLLY_NULLABLE writer);

/// Retrives the current runtime stats writer.
BaseRuntimeStatWriter* FOLLY_NULLABLE getThreadLocalRunTimeStatWriter();

/// Writes runtime counter to the current Operator running on that thread.
void addThreadLocalRuntimeStat(
    const std::string& name,
    const RuntimeCounter& value);

/// Scope guard to conveniently set and revert back the current stat writer.
class RuntimeStatWriterScopeGuard {
 public:
  explicit RuntimeStatWriterScopeGuard(
      BaseRuntimeStatWriter* FOLLY_NULLABLE writer)
      : prevWriter_(getThreadLocalRunTimeStatWriter()) {
    setThreadLocalRunTimeStatWriter(writer);
  }

  ~RuntimeStatWriterScopeGuard() {
    setThreadLocalRunTimeStatWriter(prevWriter_);
  }

 private:
  BaseRuntimeStatWriter* const FOLLY_NULLABLE prevWriter_;
};

} // namespace facebook::velox
template <>
struct fmt::formatter<facebook::velox::RuntimeCounter::Unit> : formatter<int> {
  auto format(facebook::velox::RuntimeCounter::Unit s, format_context& ctx)
      const {
    return formatter<int>::format(static_cast<int>(s), ctx);
  }
};
