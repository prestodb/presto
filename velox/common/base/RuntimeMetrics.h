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

  void addValue(int64_t value);

  void printMetric(std::stringstream& stream) const;

  void merge(const RuntimeMetric& other);
};
} // namespace facebook::velox
