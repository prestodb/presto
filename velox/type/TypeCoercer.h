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

#include "velox/type/Type.h"

namespace facebook::velox {

/// Type coercion necessary to bind a type to a signature.
struct Coercion {
  TypePtr type;
  int32_t cost{0};

  std::string toString() const {
    if (type == nullptr) {
      return "null";
    }

    return fmt::format("{} ({})", type->toString(), cost);
  }

  void reset() {
    type = nullptr;
    cost = 0;
  }

  /// Returns overall cost of a list of coercions by adding up individual costs.
  static int64_t overallCost(const std::vector<Coercion>& coercions);

  /// Returns an index of the lowest cost coercion in 'candidates' or nullptr if
  /// 'candidates' is empty or there is a tie.
  template <typename T>
  static std::optional<size_t> pickLowestCost(
      const std::vector<std::pair<std::vector<Coercion>, T>>& candidates) {
    if (candidates.empty()) {
      return std::nullopt;
    }

    if (candidates.size() == 1) {
      return 0;
    }

    std::vector<std::pair<size_t, int64_t>> costs;
    costs.reserve(candidates.size());
    for (auto i = 0; i < candidates.size(); ++i) {
      costs.emplace_back(i, overallCost(candidates[i].first));
    }

    std::sort(costs.begin(), costs.end(), [](const auto& a, const auto& b) {
      return a.second < b.second;
    });

    if (costs[0].second < costs[1].second) {
      return costs[0].first;
    }

    return std::nullopt;
  }
};

class TypeCoercer {
 public:
  /// Checks if the base of 'fromType' can be implicitly converted to a type
  /// with the given name.
  ///
  /// @return "to" type and cost if conversion is possible.
  static std::optional<Coercion> coerceTypeBase(
      const TypePtr& fromType,
      const std::string& toTypeName);
};

} // namespace facebook::velox
