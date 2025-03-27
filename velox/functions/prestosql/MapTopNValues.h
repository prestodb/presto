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

#include "velox/expression/ComplexViewTypes.h"
#include "velox/functions/Udf.h"
#include "velox/functions/prestosql/MapTopN.h"

namespace facebook::velox::functions {

template <typename TExec>
struct MapTopNValuesFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  template <typename It>
  struct Compare {
    bool operator()(const It& l, const It& r) const {
      static const CompareFlags flags{
          /*nullsFirst=*/false,
          /*ascending=*/true,
          /*equalsOnly=*/false,
          CompareFlags::NullHandlingMode::kNullAsIndeterminate};

      // No need for tie-breaker comparison, as we only maintain top N values.
      if (l->second.has_value() && r->second.has_value()) {
        return l->second.value().compare(r->second.value(), flags) >= 0;
      }
      return l->second.has_value();
    }
  };

  void call(
      out_type<Array<Orderable<T2>>>& out,
      const arg_type<Map<Orderable<T1>, Orderable<T2>>>& inputMap,
      int64_t n) {
    VELOX_USER_CHECK_GE(n, 0, "n must be greater than or equal to 0");

    if (n == 0) {
      return;
    }

    using It = typename arg_type<Map<Orderable<T1>, Orderable<T2>>>::Iterator;
    using Compare = Compare<It>;

    const Compare comparator;
    std::priority_queue<It, std::vector<It>, Compare> topEntries;
    for (auto it = inputMap.begin(); it != inputMap.end(); ++it) {
      if (topEntries.size() < n) {
        topEntries.push(it);
      } else if (comparator(it, topEntries.top())) {
        topEntries.pop();
        topEntries.push(it);
      }
    }

    std::vector<It> result;
    result.reserve(topEntries.size());
    while (!topEntries.empty()) {
      result.push_back(topEntries.top());
      topEntries.pop();
    }

    // Output the results in descending order.
    for (auto it = result.crbegin(); it != result.crend(); ++it) {
      out.push_back((*it)->second);
    }
  }
};

} // namespace facebook::velox::functions
