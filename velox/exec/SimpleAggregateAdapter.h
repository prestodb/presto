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
  // `default_null_behavior_`.
  // All accumulators are initialized to NULL before the aggregation starts.
  // However, for functions that have default and non-default null behaviors,
  // there are a couple of differences in their implementations.
  // 1. When default_null_behavior_ is true, authors define
  //     void AccumulatorType::addInput(HashStringAllocator* allocator,
  //                                    exec::arg_type<T1> arg1, ...)
  //     void AccumulatorType::combine(HashStringAllocator* allocator,
  //                                   exec::arg_type<IntermediateType> arg)
  // These functions only receive non-null input values. Input rows that contain
  // at least one NULL argument are ignored. The accumulator of a group is set
  // to non-null if at least one input is added to this group through addInput()
  // or combine(). Similarly, authors define
  //     bool AccumulatorType::writeIntermediateResult(
  //         exec::out_type<IntermediateType>&out)
  //     bool AccumulatorType::writeFinalResult(exec::out_type<OutputType>&out)
  // These functions are only called on groups of non-null accumulators. Groups
  // that have NULL accumulators automatically become NULL in the result vector.
  // These functions also return a bool indicating whether the current group
  // should be a NULL in the result vector.
  //
  // 2. When default_null_behavior_ is false, authors define
  //     bool AccumulatorType::addInput(HashStringAllocator* allocator,
  //                                    exec::optional_arg_type<T1> arg1, ...)
  //     bool AccumulatorType::combine(
  //         HashStringAllocator* allocator,
  //         exec::optional_arg_type<IntermediateType> arg)
  // These functions receive both non-null and null inputs. They return a bool
  // indicating whether to set the current group's accumulator to non-null. If
  // the accumulator of a group is already non-NULL, returning false from
  // addInput() or combine() doesn't change this group's nullness. Authors also
  // define
  //     bool AccumulatorType::writeIntermediateResult(
  //         bool nonNullGroup,
  //         exec::out_type<IntermediateType>& out)
  //     bool AccumulatorType::writeFinalResult(
  //         bool nonNullGroup,
  //         exec::out_type<OutputType>& out)
  // These functions are called on groups of both non-null and null
  // accumulators. These functions also return a bool indicating whether the
  // current group should be a NULL in the result vector.
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

  // Whether the accumulator type defines its destroy() method or not. If it is
  // defined, we call the accumulator's destroy() in
  // SimpleAggregateAdapter::destroy().
  template <typename T, typename = void>
  struct accumulator_custom_destroy : std::false_type {};

  template <typename T>
  struct accumulator_custom_destroy<T, std::void_t<decltype(&T::destroy)>>
      : std::true_type {};

  // Whether the function defines its toIntermediate() method or not. If it is
  // defined, SimpleAggregateAdapter::supportToIntermediate() returns true.
  // Otherwise, SimpleAggregateAdapter::supportToIntermediate() returns false
  // and SimpleAggregateAdapter::toIntermediate() is empty.
  template <typename T, typename = void>
  struct support_to_intermediate : std::false_type {};

  template <typename T>
  struct support_to_intermediate<T, std::void_t<decltype(&T::toIntermediate)>>
      : std::true_type {};

  static constexpr bool aggregate_default_null_behavior_ =
      aggregate_default_null_behavior<FUNC>::value;

  static constexpr bool accumulator_is_fixed_size_ =
      accumulator_is_fixed_size<typename FUNC::AccumulatorType>::value;

  static constexpr bool accumulator_use_external_memory_ =
      accumulator_use_external_memory<typename FUNC::AccumulatorType>::value;

  static constexpr bool accumulator_custom_destroy_ =
      accumulator_custom_destroy<typename FUNC::AccumulatorType>::value;

  static constexpr bool support_to_intermediate_ =
      support_to_intermediate<FUNC>::value;

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

  bool supportsToIntermediate() const override {
    return support_to_intermediate_;
  }

  void toIntermediate(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      VectorPtr& result) const override {
    if constexpr (support_to_intermediate_) {
      std::vector<DecodedVector> inputDecoded{args.size()};
      for (column_index_t i = 0; i < args.size(); ++i) {
        inputDecoded[i].decode(*args[i], rows);
      }

      toIntermediateImpl(
          inputDecoded,
          rows,
          result,
          std::make_index_sequence<FUNC::InputType::size_>{});
    } else {
      VELOX_UNREACHABLE(
          "toIntermediate should only be called when support_to_intermediate_ is true.");
    }
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
    if constexpr (accumulator_custom_destroy_) {
      for (auto group : groups) {
        auto accumulator = value<typename FUNC::AccumulatorType>(group);
        if (!isNull(group)) {
          accumulator->destroy(allocator_);
        }
      }
    }
    destroyAccumulators<typename FUNC::AccumulatorType>(groups);
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
        std::optional<RowSizeTracker<char, uint32_t>> tracker;
        if constexpr (!accumulator_is_fixed_size_) {
          tracker.emplace(groups[row][rowSizeOffset_], *allocator_);
        }
        auto group = value<typename FUNC::AccumulatorType>(groups[row]);
        group->addInput(allocator_, std::get<Is>(readers)[row]...);
        clearNull(groups[row]);
      });
    } else {
      rows.applyToSelected([&](auto row) {
        std::optional<RowSizeTracker<char, uint32_t>> tracker;
        if constexpr (!accumulator_is_fixed_size_) {
          tracker.emplace(groups[row][rowSizeOffset_], *allocator_);
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
        std::optional<RowSizeTracker<char, uint32_t>> tracker;
        if constexpr (!accumulator_is_fixed_size_) {
          tracker.emplace(group[rowSizeOffset_], *allocator_);
        }
        accumulator->addInput(allocator_, std::get<Is>(readers)[row]...);
        clearNull(group);
      });
    } else {
      rows.applyToSelected([&](auto row) {
        std::optional<RowSizeTracker<char, uint32_t>> tracker;
        if constexpr (!accumulator_is_fixed_size_) {
          tracker.emplace(group[rowSizeOffset_], *allocator_);
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

  template <std::size_t... Is>
  void toIntermediateImpl(
      const std::vector<DecodedVector>& inputDecoded,
      const SelectivityVector& rows,
      VectorPtr& result,
      std::index_sequence<Is...>) const {
    std::tuple<VectorReader<typename FUNC::InputType::template type_at<Is>>...>
        readers{&inputDecoded[Is]...};

    VELOX_CHECK(result);
    result->ensureWritable(rows);
    auto* rawNulls = result->mutableRawNulls();
    bits::fillBits(rawNulls, 0, result->size(), bits::kNull);

    constexpr auto intermediateKind =
        SimpleTypeTrait<typename FUNC::IntermediateType>::typeKind;
    auto* flatResult =
        result->as<typename KindToFlatVector<intermediateKind>::type>();
    exec::VectorWriter<typename FUNC::IntermediateType> writer;
    writer.init(*flatResult);

    if constexpr (aggregate_default_null_behavior_) {
      rows.applyToSelected([&](auto row) {
        writer.setOffset(row);
        // If any input is null, we ignore the whole row.
        if (!(std::get<Is>(readers).isSet(row) && ...)) {
          writer.commitNull();
          return;
        }
        bool nonNull = FUNC::toIntermediate(
            writer.current(), std::get<Is>(readers)[row]...);
        writer.commit(nonNull);
      });
      writer.finish();
    } else {
      rows.applyToSelected([&](auto row) {
        writer.setOffset(row);
        bool nonNull = FUNC::toIntermediate(
            writer.current(),
            OptionalAccessor<typename FUNC::InputType::template type_at<Is>>{
                &std::get<Is>(readers), (int64_t)row}...);
        writer.commit(nonNull);
      });
      writer.finish();
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
        std::optional<RowSizeTracker<char, uint32_t>> tracker;
        if constexpr (!accumulator_is_fixed_size_) {
          tracker.emplace(groups[row][rowSizeOffset_], *allocator_);
        }
        auto group = value<typename FUNC::AccumulatorType>(groups[row]);
        group->combine(allocator_, reader[row]);
        clearNull(groups[row]);
      });
    } else {
      rows.applyToSelected([&](auto row) {
        std::optional<RowSizeTracker<char, uint32_t>> tracker;
        if constexpr (!accumulator_is_fixed_size_) {
          tracker.emplace(groups[row][rowSizeOffset_], *allocator_);
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
        std::optional<RowSizeTracker<char, uint32_t>> tracker;
        if constexpr (!accumulator_is_fixed_size_) {
          tracker.emplace(group[rowSizeOffset_], *allocator_);
        }
        accumulator->combine(allocator_, reader[row]);
        clearNull(group);
      });
    } else {
      rows.applyToSelected([&](auto row) {
        std::optional<RowSizeTracker<char, uint32_t>> tracker;
        if constexpr (!accumulator_is_fixed_size_) {
          tracker.emplace(group[rowSizeOffset_], *allocator_);
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
