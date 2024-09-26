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

#include "velox/functions/prestosql/aggregates/AggregateNames.h"
#include "velox/functions/prestosql/aggregates/MinMaxByAggregateBase.h"

namespace facebook::velox::aggregate::prestosql {

template <typename V, typename C>
class MaxByNAggregate : public MinMaxByNAggregate<V, C, Greater<V, C>> {
 public:
  explicit MaxByNAggregate(TypePtr resultType)
      : MinMaxByNAggregate<V, C, Greater<V, C>>(resultType) {}
};

template <typename C>
class MaxByNAggregate<ComplexType, C>
    : public MinMaxByNAggregate<
          ComplexType,
          C,
          Greater<HashStringAllocator::Position, C>> {
 public:
  explicit MaxByNAggregate(TypePtr resultType)
      : MinMaxByNAggregate<
            ComplexType,
            C,
            Greater<HashStringAllocator::Position, C>>(resultType) {}
};

void registerMaxByAggregates(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite) {
  registerMinMaxBy<
      functions::aggregate::MinMaxByAggregateBase,
      true,
      MaxByNAggregate>(prefix + kMaxBy, withCompanionFunctions, overwrite);
}

} // namespace facebook::velox::aggregate::prestosql
