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

namespace facebook::velox::functions {

template <typename TExec, typename Compare>
struct MapTopNImpl {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  using It = typename arg_type<Map<Orderable<T1>, Orderable<T2>>>::Iterator;

  void call(
      out_type<Array<Orderable<T1>>>& out,
      const arg_type<Map<Orderable<T1>, Orderable<T2>>>& inputMap,
      int64_t n) {
    VELOX_USER_CHECK_GE(n, 0, "n must be greater than or equal to 0");

    if (n == 0 || inputMap.size() == 0) {
      return;
    }

    Compare comparator;
    std::priority_queue<It, std::vector<It>, Compare> topEntries(comparator);

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
    // Reverse the order of the result to be in descending order.
    for (int i = result.size() - 1; i >= 0; i--) {
      out.push_back(result[i]->first);
    }
  }
};

} // namespace facebook::velox::functions
