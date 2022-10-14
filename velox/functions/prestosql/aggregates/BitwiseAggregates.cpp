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

#include "velox/exec/Aggregate.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/functions/prestosql/aggregates/AggregateNames.h"
#include "velox/functions/prestosql/aggregates/SimpleNumericAggregate.h"

namespace facebook::velox::aggregate::prestosql {

namespace {

template <typename T>
class BitwiseAndOrAggregate : public SimpleNumericAggregate<T, T, T> {
  using BaseAggregate = SimpleNumericAggregate<T, T, T>;

 public:
  BitwiseAndOrAggregate(TypePtr resultType, T initialValue)
      : BaseAggregate(resultType), initialValue_(initialValue) {}

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(T);
  }

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    exec::Aggregate::setAllNulls(groups, indices);
    for (auto i : indices) {
      *exec::Aggregate::value<T>(groups[i]) = initialValue_;
    }
  }

  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    BaseAggregate::doExtractValues(groups, numGroups, result, [&](char* group) {
      return *BaseAggregate::Aggregate::template value<T>(group);
    });
  }

  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    this->addRawInput(groups, rows, args, mayPushdown);
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool mayPushdown) override {
    this->addSingleGroupRawInput(group, rows, args, mayPushdown);
  }

 protected:
  const T initialValue_;
};

template <typename T>
class BitwiseOrAggregate : public BitwiseAndOrAggregate<T> {
 public:
  explicit BitwiseOrAggregate(TypePtr resultType)
      : BitwiseAndOrAggregate<T>(
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
class BitwiseAndAggregate : public BitwiseAndOrAggregate<T> {
 public:
  explicit BitwiseAndAggregate(TypePtr resultType)
      : BitwiseAndOrAggregate<T>(
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

template <template <typename U> class T>
bool registerBitwiseAggregate(const std::string& name) {
  // TODO Fix the signatures to match Presto.
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures;
  for (const auto& inputType : {"tinyint", "smallint", "integer", "bigint"}) {
    signatures.push_back(exec::AggregateFunctionSignatureBuilder()
                             .returnType(inputType)
                             .intermediateType(inputType)
                             .argumentType(inputType)
                             .build());
  }

  exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType) -> std::unique_ptr<exec::Aggregate> {
        VELOX_CHECK_LE(argTypes.size(), 1, "{} takes only one argument", name);
        auto inputType = argTypes[0];
        switch (inputType->kind()) {
          case TypeKind::TINYINT:
            return std::make_unique<T<int8_t>>(resultType);
          case TypeKind::SMALLINT:
            return std::make_unique<T<int16_t>>(resultType);
          case TypeKind::INTEGER:
            return std::make_unique<T<int32_t>>(resultType);
          case TypeKind::BIGINT:
            return std::make_unique<T<int64_t>>(resultType);
          default:
            VELOX_CHECK(
                false,
                "Unknown input type for {} aggregation {}",
                name,
                inputType->kindName());
        }
      });
  return true;
}

} // namespace

void registerBitwiseAggregates() {
  registerBitwiseAggregate<BitwiseOrAggregate>(kBitwiseOr);
  registerBitwiseAggregate<BitwiseAndAggregate>(kBitwiseAnd);
}

} // namespace facebook::velox::aggregate::prestosql
