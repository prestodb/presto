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

#include "velox/functions/lib/aggregates/BitwiseAggregateBase.h"
#include "velox/functions/prestosql/aggregates/AggregateNames.h"

using namespace facebook::velox::functions::aggregate;

namespace facebook::velox::aggregate::prestosql {

namespace {

template <typename T>
class BitwiseOrAggregate : public BitwiseAggregateBase<T> {
 public:
  explicit BitwiseOrAggregate(TypePtr resultType)
      : BitwiseAggregateBase<T>(
            resultType,
            /* initialValue = */ 0) {}

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    SimpleNumericAggregate<T, T, T>::template updateGroups<true>(
        groups,
        rows,
        args[0],
        [](T& result, T value) { result |= value; },
        mayPushdown);
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    SimpleNumericAggregate<T, T, T>::updateOneGroup(
        group,
        rows,
        args[0],
        [](T& result, T value) { result |= value; },
        [](T& result, T value, int /* unused */
        ) { result |= value; },
        mayPushdown,
        this->initialValue_);
  }
};

template <typename T>
class BitwiseAndAggregate : public BitwiseAggregateBase<T> {
 public:
  explicit BitwiseAndAggregate(TypePtr resultType)
      : BitwiseAggregateBase<T>(
            resultType,
            /* initialValue = */ -1) {}

  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    SimpleNumericAggregate<T, T, T>::template updateGroups<true>(
        groups,
        rows,
        args[0],
        [](T& result, T value) { result &= value; },
        mayPushdown);
  }

  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    SimpleNumericAggregate<T, T, T>::template updateOneGroup(
        group,
        rows,
        args[0],
        [](T& result, T value) { result &= value; },
        [](T& result, T value, int /* unused */
        ) { result &= value; },
        mayPushdown,
        this->initialValue_);
  }
};

} // namespace

void registerBitwiseAggregates(
    const std::string& prefix,
    bool onlyPrestoSignatures) {
  registerBitwise<BitwiseOrAggregate>(
      prefix + kBitwiseOr, onlyPrestoSignatures);
  registerBitwise<BitwiseAndAggregate>(
      prefix + kBitwiseAnd, onlyPrestoSignatures);
}

} // namespace facebook::velox::aggregate::prestosql
