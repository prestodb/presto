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
#include "velox/type/TypeCoercer.h"

namespace facebook::velox {

int64_t Coercion::overallCost(const std::vector<Coercion>& coercions) {
  int64_t cost = 0;
  for (const auto& coercion : coercions) {
    if (coercion.type != nullptr) {
      cost += coercion.cost;
    }
  }

  return cost;
}

namespace {

std::unordered_map<std::pair<std::string, std::string>, Coercion>
allowedCoercions() {
  std::unordered_map<std::pair<std::string, std::string>, Coercion> coercions;

  auto add = [&](const TypePtr& from, const std::vector<TypePtr>& to) {
    int32_t cost = 0;
    for (const auto& toType : to) {
      coercions.emplace(
          std::make_pair<std::string, std::string>(
              from->kindName(), toType->kindName()),
          Coercion{.type = toType, .cost = ++cost});
    }
  };

  add(TINYINT(), {SMALLINT(), INTEGER(), BIGINT(), REAL(), DOUBLE()});
  add(SMALLINT(), {INTEGER(), BIGINT(), REAL(), DOUBLE()});
  add(INTEGER(), {BIGINT(), REAL(), DOUBLE()});
  add(BIGINT(), {DOUBLE()});
  add(REAL(), {DOUBLE()});

  return coercions;
}
} // namespace

// static
std::optional<Coercion> TypeCoercer::coerceTypeBase(
    const TypePtr& fromType,
    const std::string& toTypeName) {
  static const auto kAllowedCoercions = allowedCoercions();
  if (fromType->name() == toTypeName) {
    return Coercion{.type = fromType, .cost = 0};
  }

  auto it = kAllowedCoercions.find({fromType->name(), toTypeName});
  if (it != kAllowedCoercions.end()) {
    return it->second;
  }

  return std::nullopt;
}

} // namespace facebook::velox
