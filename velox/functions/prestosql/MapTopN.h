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

template <typename TExec>
struct MapTopNFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  template <typename It>
  struct Compare {
    bool operator()(const It& l, const It& r) const {
      static const CompareFlags flags{
          false /*nullsFirst*/,
          true /*ascending*/,
          false /*equalsOnly*/,
          CompareFlags::NullHandlingMode::kNullAsIndeterminate};

      if (l->second.has_value() && r->second.has_value()) {
        auto comp = l->second.value().compare(r->second.value(), flags);

        if (FOLLY_UNLIKELY(comp == 0)) {
          return l->first.compare(r->first, flags) > 0;
        }

        return comp > 0;
      } else if (FOLLY_UNLIKELY(
                     l->second.has_value() && r->second.has_value())) {
        return l->first.compare(r->first, flags) > 0;
      }

      return l->second.has_value();
    }
  };

  void call(
      out_type<Map<Orderable<T1>, Orderable<T2>>>& out,
      const arg_type<Map<Orderable<T1>, Orderable<T2>>>& inputMap,
      int64_t n) {
    VELOX_USER_CHECK_GE(n, 0, "n must be greater than or equal to 0")

    if (n == 0) {
      return;
    }

    if (n >= inputMap.size()) {
      out.copy_from(inputMap);
      return;
    }

    using It = typename arg_type<Map<Orderable<T1>, Orderable<T2>>>::Iterator;

    Compare<It> comparator;

    std::priority_queue<It, std::vector<It>, Compare<It>> topEntries(
        comparator);

    for (auto it = inputMap.begin(); it != inputMap.end(); ++it) {
      if (topEntries.size() < n) {
        topEntries.push(it);
      } else if (comparator(it, topEntries.top())) {
        topEntries.pop();
        topEntries.push(it);
      }
    }

    while (!topEntries.empty()) {
      auto it = topEntries.top();

      if (!it->second.has_value()) {
        auto& keyWriter = out.add_null();
        keyWriter.copy_from(it->first);
      } else {
        auto [keyWriter, valueWriter] = out.add_item();
        keyWriter.copy_from(it->first);
        valueWriter.copy_from(it->second.value());
      }

      topEntries.pop();
    }
  }
};

} // namespace facebook::velox::functions
