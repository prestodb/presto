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

#include "velox/exec/Aggregate.h"
#include "velox/expression/VectorReaders.h"
#include "velox/expression/VectorWriters.h"

namespace facebook::velox::exec {

// The writer type of T used in simple UDAF interface. An instance of
// out_type allows writing one row into the output vector.
template <typename T>
using out_type = typename VectorExec::template resolver<T>::out_type;

// The reader type of T used in simple UDAF interface. An instance of
// arg_type allows reading one row from the input vector. This is used for UDAFs
// that have the default null behavior.
template <typename T>
using arg_type = typename VectorExec::template resolver<T>::in_type;

// The reader type of T used in simple UDAF interface. An instance of
// arg_type allows reading one row from the input vector. This is used for UDAFs
// that have non-default null behavior.
template <typename T>
using optional_arg_type = OptionalAccessor<T>;

template <typename FUNC>
class SimpleAggregateAdapter : public Aggregate {
 public:
  explicit SimpleAggregateAdapter(TypePtr resultType) : Aggregate(resultType) {}

  // Assume most aggregate functions have fixed-size accumulators. Functions
  // that
  // have non-fixed-size accumulators should overwrite `is_fixed_size_` in their
  // accumulator structs.
  template <typename T, typename = void>
  struct accumulator_is_fixed_size : std::true_type {};

  template <typename T>
  struct accumulator_is_fixed_size<T, std::void_t<decltype(T::is_fixed_size_)>>
      : std::integral_constant<bool, T::is_fixed_size_> {};

  // Assume most aggregate functions have default null behavior, i.e., ignoring
  // rows that have null values in raw input and intermediate results, and
  // returning null for groups of no input rows or only null rows.
  // For example: select sum(col0)
  //              from (values (1, 10), (2, 20), (3, 30)) as t(col0, col1)
  //              where col0 > 10; -- NULL
  // Functions that have non-default null behavior should overwrite
  // `default_null_behavior_` in their classes. In this case,
  // AccumulatorType::addInput() and AccumulatorType::combine() return a boolean
  // indicating whether the current value makes the accumulator of its
  // corresponding group non-null. If the accumulator of a group is already
  // non-null, returning false from addInput() or combine() doesn't change this
  // group's nullness.
  template <typename T, typename = void>
  struct aggregate_default_null_behavior : std::true_type {};

  template <typename T>
  struct aggregate_default_null_behavior<
      T,
      std::void_t<decltype(T::default_null_behavior_)>>
      : std::integral_constant<bool, T::default_null_behavior_> {};

  // Assume most aggregate functions do not use external memory. Functions that
  // use external memory should overwrite `use_external_memory_` in their
  // accumulator structs.
  template <typename T, typename = void>
  struct accumulator_use_external_memory : std::false_type {};

  template <typename T>
  struct accumulator_use_external_memory<
      T,
      std::void_t<decltype(T::use_external_memory_)>>
      : std::integral_constant<bool, T::use_external_memory_> {};

  static constexpr bool aggregate_default_null_behavior_ =
      aggregate_default_null_behavior<FUNC>::value;

  static constexpr bool accumulator_is_fixed_size_ =
      accumulator_is_fixed_size<typename FUNC::AccumulatorType>::value;

  static constexpr bool accumulator_use_external_memory_ =
      accumulator_use_external_memory<typename FUNC::AccumulatorType>::value;

  bool isFixedSize() const override {
    return accumulator_is_fixed_size_;
  }

  bool accumulatorUsesExternalMemory() const override {
    return accumulator_use_external_memory_;
  }

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(typename FUNC::AccumulatorType);
  }

  void initializeNewGroups(
      char** groups,
      folly::Range<const vector_size_t*> indices) override {
    setAllNulls(groups, indices);
    for (auto i : indices) {
      new (groups[i] + offset_) typename FUNC::AccumulatorType(allocator_);
    }
  }

  // Add raw input to accumulators. If the simple aggregation function has
  // default null behavior, input rows that has nulls are skipped. Otherwise,
  // the accumulator type's addInput() method handles null inputs.
  void addRawInput(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    if (inputDecoded_.size() < args.size()) {
      inputDecoded_.resize(args.size());
    }

    for (column_index_t i = 0; i < args.size(); ++i) {
      inputDecoded_[i].decode(*args[i], rows);
    }

    addRawInputImpl(
        groups, rows, std::make_index_sequence<FUNC::InputType::size_>{});
  }

  // Similar to addRawInput, but add inputs to one single accumulator.
  void addSingleGroupRawInput(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    if (inputDecoded_.size() < args.size()) {
      inputDecoded_.resize(args.size());
    }

    for (column_index_t i = 0; i < args.size(); ++i) {
      inputDecoded_[i].decode(*args[i], rows);
    }

    addSingleGroupRawInputImpl(
        group, rows, std::make_index_sequence<FUNC::InputType::size_>{});
  }

  // Add intermediate results to accumulators. If the simple aggregation
  // function has default null behavior, intermediate result rows that has nulls
  // are skipped. Otherwise, the accumulator type's combine() method handles
  // nulls.
  void addIntermediateResults(
      char** groups,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /*mayPushdown*/) override {
    intermediateDecoded_.decode(*args[0], rows);

    addIntermediateResultsImpl(groups, rows);
  }

  // Similar to addIntermediateResults, but add intermediate results to one
  // single accumulator.
  void addSingleGroupIntermediateResults(
      char* group,
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      bool /* mayPushdown */) override {
    intermediateDecoded_.decode(*args[0], rows);

    addSingleGroupIntermediateResultsImpl(group, rows);
  }

  // Extract intermediate results to a vector.
  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    VectorWriter<typename FUNC::IntermediateType> writer;
    auto vector = (*result)
                      ->as<typename TypeToFlatVector<
                          typename FUNC::IntermediateType>::type>();
    vector->resize(numGroups);
    writer.init(*vector);

    for (auto i = 0; i < numGroups; ++i) {
      writer.setOffset(i);
      auto group = value<typename FUNC::AccumulatorType>(groups[i]);

      if constexpr (aggregate_default_null_behavior_) {
        if (isNull(groups[i])) {
          writer.commitNull();
        } else {
          bool nonNull = group->writeIntermediateResult(writer.current());
          writer.commit(nonNull);
        }
      } else {
        bool nonNull = group->writeIntermediateResult(
            !isNull(groups[i]), writer.current());
        writer.commit(nonNull);
      }
    }
    writer.finish();
  }

  // Extract final results to a vector.
  void extractValues(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    auto flatResult =
        (*result)
            ->as<typename TypeToFlatVector<typename FUNC::OutputType>::type>();
    flatResult->resize(numGroups);

    VectorWriter<typename FUNC::OutputType> writer;
    writer.init(*flatResult);

    for (auto i = 0; i < numGroups; ++i) {
      writer.setOffset(i);
      auto group = value<typename FUNC::AccumulatorType>(groups[i]);

      if constexpr (aggregate_default_null_behavior_) {
        if (isNull(groups[i])) {
          writer.commitNull();
        } else {
          bool nonNull = group->writeFinalResult(writer.current());
          writer.commit(nonNull);
        }
      } else {
        bool nonNull =
            group->writeFinalResult(!isNull(groups[i]), writer.current());
        writer.commit(nonNull);
      }
    }
    writer.finish();
  }

  void destroy(folly::Range<char**> groups) override {
    for (auto group : groups) {
      auto accumulator = value<typename FUNC::AccumulatorType>(group);
      std::destroy_at(accumulator);
    }
  }

 private:
  template <std::size_t... Is>
  void addRawInputImpl(
      char** groups,
      const SelectivityVector& rows,
      std::index_sequence<Is...>) {
    std::tuple<VectorReader<typename FUNC::InputType::template type_at<Is>>...>
        readers{&inputDecoded_[Is]...};

    if constexpr (aggregate_default_null_behavior_) {
      rows.applyToSelected([&](auto row) {
        // If any input is null, we ignore the whole row.
        if (!(std::get<Is>(readers).isSet(row) && ...)) {
          return;
        }
        if constexpr (!accumulator_is_fixed_size_) {
          auto tracker = trackRowSize(groups[row]);
        }
        auto group = value<typename FUNC::AccumulatorType>(groups[row]);
        group->addInput(allocator_, std::get<Is>(readers)[row]...);
        clearNull(groups[row]);
      });
    } else {
      rows.applyToSelected([&](auto row) {
        if constexpr (!accumulator_is_fixed_size_) {
          auto tracker = trackRowSize(groups[row]);
        }
        auto group = value<typename FUNC::AccumulatorType>(groups[row]);
        bool nonNull = group->addInput(
            allocator_,
            OptionalAccessor<typename FUNC::InputType::template type_at<Is>>{
                &std::get<Is>(readers), (int64_t)row}...);
        if (nonNull) {
          clearNull(groups[row]);
        }
      });
    }
  }

  template <std::size_t... Is>
  void addSingleGroupRawInputImpl(
      char* group,
      const SelectivityVector& rows,
      std::index_sequence<Is...>) {
    std::tuple<VectorReader<typename FUNC::InputType::template type_at<Is>>...>
        readers{&inputDecoded_[Is]...};
    auto accumulator = value<typename FUNC::AccumulatorType>(group);

    if constexpr (aggregate_default_null_behavior_) {
      rows.applyToSelected([&](auto row) {
        // If any input is null, we ignore the whole row.
        if (!(std::get<Is>(readers).isSet(row) && ...)) {
          return;
        }
        if constexpr (!accumulator_is_fixed_size_) {
          auto tracker = trackRowSize(group);
        }
        accumulator->addInput(allocator_, std::get<Is>(readers)[row]...);
        clearNull(group);
      });
    } else {
      rows.applyToSelected([&](auto row) {
        if constexpr (!accumulator_is_fixed_size_) {
          auto tracker = trackRowSize(group);
        }
        bool nonNull = accumulator->addInput(
            allocator_,
            OptionalAccessor<typename FUNC::InputType::template type_at<Is>>{
                &std::get<Is>(readers), (int64_t)row}...);
        if (nonNull) {
          clearNull(group);
        }
      });
    }
  }

  // Implementation of addIntermediateResults when the intermediate type is not
  // a Row type.
  void addIntermediateResultsImpl(
      char** groups,
      const SelectivityVector& rows) {
    VectorReader<typename FUNC::IntermediateType> reader(&intermediateDecoded_);

    if constexpr (aggregate_default_null_behavior_) {
      rows.applyToSelected([&](auto row) {
        if (!reader.isSet(row)) {
          return;
        }
        if constexpr (!accumulator_is_fixed_size_) {
          auto tracker = trackRowSize(groups[row]);
        }
        auto group = value<typename FUNC::AccumulatorType>(groups[row]);
        group->combine(allocator_, reader[row]);
        clearNull(groups[row]);
      });
    } else {
      rows.applyToSelected([&](auto row) {
        if constexpr (!accumulator_is_fixed_size_) {
          auto tracker = trackRowSize(groups[row]);
        }
        auto group = value<typename FUNC::AccumulatorType>(groups[row]);
        bool nonNull = group->combine(
            allocator_,
            OptionalAccessor<typename FUNC::IntermediateType>{
                &reader, (int64_t)row});
        if (nonNull) {
          clearNull(groups[row]);
        }
      });
    }
  }

  // Implementation of addSingleGroupIntermediateResults when the intermediate
  // type is not a Row type.
  void addSingleGroupIntermediateResultsImpl(
      char* group,
      const SelectivityVector& rows) {
    VectorReader<typename FUNC::IntermediateType> reader(&intermediateDecoded_);
    auto accumulator = value<typename FUNC::AccumulatorType>(group);

    if constexpr (aggregate_default_null_behavior_) {
      rows.applyToSelected([&](auto row) {
        if (!reader.isSet(row)) {
          return;
        }
        if constexpr (!accumulator_is_fixed_size_) {
          auto tracker = trackRowSize(group);
        }
        accumulator->combine(allocator_, reader[row]);
        clearNull(group);
      });
    } else {
      rows.applyToSelected([&](auto row) {
        if constexpr (!accumulator_is_fixed_size_) {
          auto tracker = trackRowSize(group);
        }
        bool nonNull = accumulator->combine(
            allocator_,
            OptionalAccessor<typename FUNC::IntermediateType>{
                &reader, (int64_t)row});
        if (nonNull) {
          clearNull(group);
        }
      });
    }
  }

  std::vector<DecodedVector> inputDecoded_;
  DecodedVector intermediateDecoded_;
};

} // namespace facebook::velox::exec
