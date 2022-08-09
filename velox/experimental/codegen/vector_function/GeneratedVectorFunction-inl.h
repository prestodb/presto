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

#include <iostream>
#include <thread>
#include <type_traits>
#include <utility>
#include "velox/experimental/codegen/vector_function/ConcatExpression-inl.h"
#include "velox/experimental/codegen/vector_function/Perf.h"
#include "velox/experimental/codegen/vector_function/VectorReader-inl.h"
#include "velox/expression/VectorFunction.h"

namespace facebook {
namespace velox {
namespace codegen {

template <bool isDefaultNull, bool isDefaultNullStrict>
struct InputReaderConfig {
  static constexpr bool isWriter_ = false;
  // when true, the reader will never read a nullvalue
  static constexpr bool mayReadNull_ = !isDefaultNull && !isDefaultNullStrict;

  // irrelevent for reader
  static constexpr bool mayWriteNull_ = false;
  static constexpr bool intializedWithNullSet_ = false;

  constexpr static bool inputStringBuffersShared = false;
  constexpr static bool constantStringBuffersShared = false;
};

template <bool isDefaultNull, bool isDefaultNullStrict>
struct OutputReaderConfig {
  static constexpr bool isWriter_ = true;
  // true means set to null, false means not null
  static constexpr bool intializedWithNullSet_ =
      isDefaultNull || isDefaultNullStrict;
  // when true, the reader will never reveive a null value to write
  static constexpr bool mayWriteNull_ = !isDefaultNullStrict;
  static constexpr bool mayReadNull_ = mayWriteNull_;

  constexpr static bool inputStringBuffersShared = true;
  constexpr static bool constantStringBuffersShared = false;
};

// #define DEBUG_CODEGEN
// Debugging/printing functions
namespace {
template <typename ReferenceType>
std::ostream& printReference(
    std::ostream& out,
    const ReferenceType& reference) {
  auto value = reference.toOptionalValueType();

  std::stringstream referenceStream;
  if (!value.has_value()) {
    referenceStream << "Null";
  } else {
    referenceStream << value.value();
  }
  out << fmt::format(
      "ReferenceType [ rowIndex {}, vector address {}, value {} ]",
      reference.rowIndex_,
      static_cast<void*>(reference.reader_.vector_.get()),
      referenceStream.str());
  return out;
}

template <typename ValType>
std::ostream& printValue(std::ostream& out, const ValType& value) {
  std::stringstream referenceStream;
  if (!value.has_value()) {
    referenceStream << "Null";
  } else {
    referenceStream << value.value();
  }
  out << fmt::format("[ value {} ]", referenceStream.str());
  return out;
}

template <typename... Types>
void printTuple(const std::tuple<Types...>& tuple) {
  auto printElement = [](/*auto && first,*/ auto&&... elements) {
    (printValue(std::cerr, elements), ...);
  };
  std::apply(printElement, tuple);
};

// HelperFunction
template <typename... Types>
std::ostream& operator<<(std::ostream& out, const std::tuple<Types...>& tuple) {
  auto printElement = [&out](auto&& first, auto&&... elements) {
    out << std::forward<decltype(first)>(first);
    ((out << ", " << std::forward<decltype(elements)>(elements)), ...);
  };
  std::apply(printElement, tuple);
  return out;
};
} // namespace

/// Base class for all generated function
/// This add a new apply method to support multiple output;
class GeneratedVectorFunctionBase
    : public facebook::velox::exec::VectorFunction {
 public:
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Woverloaded-virtual"
  // Multiple output apply method.
  virtual size_t apply(
      const facebook::velox::SelectivityVector& rows,
      std::vector<facebook::velox::VectorPtr>& args,
      const TypePtr& /* outputType */,
      facebook::velox::exec::EvalCtx* context,
      std::vector<facebook::velox::VectorPtr>& results) const = 0;
#pragma clang diagnostic pop

  void setRowType(const std::shared_ptr<const RowType>& rowType) {
    rowType_ = rowType;
  }

 protected:
  std::shared_ptr<const RowType> rowType_;
};

// helper templates/functions
namespace {
template <typename Func, typename T, std::size_t... Indices>
void applyLambdaToVector(
    Func func,
    std::vector<T>& args,
    [[maybe_unused]] const std::index_sequence<Indices...>& unused) {
  (func(args[Indices]), ...);
}

template <size_t... Indices>
constexpr bool indexIn(const size_t i, const std::index_sequence<Indices...>&) {
  return ((i == Indices) || ...);
}
} // namespace

///
/// \tparam GeneratedCode
template <typename GeneratedCodeConfig>
class GeneratedVectorFunction : public GeneratedVectorFunctionBase {
 public:
  using GeneratedCode = typename GeneratedCodeConfig::GeneratedCodeClass;
  using OutputTypes = typename GeneratedCode::VeloxOutputType;
  using InputTypes = typename GeneratedCode::VeloxInputType;

  // only useful when there's filter
  using FilterInputIndices =
      typename GeneratedCode::template InputMapAtIndex<0>;

  GeneratedVectorFunction() : generatedExpression_() {}

 public:
  virtual void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx* context,
      VectorPtr* result) const override {
    VELOX_CHECK(result != nullptr);
    VELOX_CHECK(rowType_ != nullptr);
    VELOX_CHECK(
        (*result == nullptr) or (result->get()->as<RowVector>() != nullptr));

    // all-constant expressions shouldn't be compiled
    VELOX_CHECK(args.size() > 0);

    BaseVector::ensureWritable(rows, rowType_, context->pool(), result);

    // TODO: We should probably move this loop inside ensureWritable
    for (size_t columnIndex = 0; columnIndex < rowType_->size();
         ++columnIndex) {
      BaseVector::ensureWritable(
          rows,
          rowType_->childAt(columnIndex),
          context->pool(),
          &(result->get()->as<RowVector>()->childAt(columnIndex)));
    }

    // Constuct nulls
    for (auto& child : result->get()->as<RowVector>()->children()) {
      child->setCodegenOutput();
      if constexpr (
          (GeneratedCodeConfig::isDefaultNull ||
           GeneratedCodeConfig::isDefaultNullStrict) &&
          !GeneratedCode::hasFilter) {
        // preset all with nulls, only if it's projection only
        child->addNulls(nullptr, rows);
      } else {
        // preset all not nulls
        child->mutableRawNulls();
      }
    }

    (*result)->setCodegenOutput();

    VELOX_CHECK(result->get()->as<RowVector>() != nullptr);

    // Shared string input buffer
    // TODO: write now we are sharing everything, this not ideal. We should do
    // a static analysis reachability pass and acquire shared buffers
    // accordingly to reduce memory lifetime.
    for (auto& arg : args) {
      if (arg->type()->kind() == TypeKind::VARCHAR) {
        for (size_t columnIndex = 0; columnIndex < rowType_->size();
             ++columnIndex) {
          // Ensures that the results vectors are nullables.
          if (result->get()
                  ->as<RowVector>()
                  ->childAt(columnIndex)
                  ->type()
                  ->kind() == TypeKind::VARCHAR) {
            result->get()
                ->as<RowVector>()
                ->childAt(columnIndex)
                ->template asFlatVector<StringView>()
                ->acquireSharedStringBuffers(arg.get());
          }
        }
      }
    }

    size_t resultSize;

    if constexpr (
        GeneratedCodeConfig::isDefaultNull ||
        GeneratedCodeConfig::isDefaultNullStrict) {
      // only proccess indices where all inputs are not nulls
      auto rowsNotNull = rows;

      auto deselectNull = [&](const VectorPtr& arg) {
        if (arg->mayHaveNulls()) {
          exec::LocalDecodedVector decodedVector(context, *arg, rowsNotNull);
          if (auto* rawNulls = decodedVector->nulls()) {
            rowsNotNull.deselectNulls(rawNulls, rows.begin(), rows.end());
          }
        }
      };

      if constexpr (GeneratedCode::hasFilter) {
        /// when it's filter default null, we only deselect null bits on filter
        /// attributes even if projection's also default null, because input
        /// index and output index don't match now.
        applyLambdaToVector(deselectNull, args, FilterInputIndices{});
      } else {
        for (const auto& arg : args) {
          deselectNull(arg);
        }
      }

      resultSize = apply(
          rowsNotNull,
          args,
          outputType,
          context,
          result->get()->as<RowVector>()->children());

    } else {
      resultSize = apply(
          rows,
          args,
          outputType,
          context,
          result->get()->as<RowVector>()->children());
    }

    // truncate result
    if (resultSize != args[0]->size()) {
      for (size_t columnIndex = 0; columnIndex < rowType_->size();
           ++columnIndex) {
        result->get()
            ->as<RowVector>()
            ->childAt(columnIndex)
            ->resize(resultSize);
      }
      result->get()->resize(resultSize);
    }
  }

  size_t apply(
      const facebook::velox::SelectivityVector& rows,
      std::vector<facebook::velox::VectorPtr>& args,
      const TypePtr& /* outputType */,
      facebook::velox::exec::EvalCtx* context,
      std::vector<facebook::velox::VectorPtr>& results) const override {
    VELOX_CHECK(rowType_ != nullptr);
    VELOX_CHECK(context != nullptr);
    VELOX_CHECK(results.size() == rowType_->size());

    auto inReaders = createReaders(
        args,
        std::make_integer_sequence<
            std::size_t,
            std::tuple_size_v<InputTypes>>{});

    auto outReaders = createWriters(
        results,
        std::make_integer_sequence<
            std::size_t,
            std::tuple_size_v<OutputTypes>>{});

    size_t outIndex = 0;
    auto computeRow =
        [&inReaders, &outReaders, this, &outIndex](size_t rowIndex) {
          // Input tuples
          auto inputs = applyToTuple(
              [rowIndex](auto&& reader) -> auto {
                return (const typename std::remove_reference_t<
                        decltype(reader)>::InputType)reader[rowIndex];
              },
              inReaders,
              std::make_integer_sequence<
                  std::size_t,
                  std::tuple_size_v<InputTypes>>{});

#ifdef DEBUG_CODEGEN
          // Debugging code
          std::cerr << "Input : ";
          printTuple(inputs);
          std::cerr << std::endl;
#endif

          // Output tuples
          if constexpr (!GeneratedCode::hasFilter) {
            // with filter, output index is computed and depends on how many
            // passed the filter, without filter, it's just input index
            outIndex = rowIndex;
          }

          auto outputs = applyToTuple(
              [outIndex](auto&& reader) ->
              typename std::remove_reference_t<decltype(reader)>::PointerType {
                return reader[outIndex];
              },
              outReaders,
              std::make_integer_sequence<
                  std::size_t,
                  std::tuple_size_v<OutputTypes>>{});

          // Apply the generated function.
          if constexpr (GeneratedCode::hasFilter) {
            // with filter, return value indicates passed or not
            if (generatedExpression_(inputs, outputs)) {
              outIndex++;
            }
          } else {
            generatedExpression_(inputs, outputs);
          }

#ifdef DEBUG_CODEGEN
          // Debugging code
          std::cerr << "Output : ";
          printTuple(outputs);
          std::cerr << std::endl;
#endif
          return true;
        };

    {
      Perf perf;
      rows.applyToSelected(computeRow);
    }

    // no filter, the return value is just original size
    if constexpr (!GeneratedCode::hasFilter) {
      outIndex = args[0]->size();
    }

    return outIndex;
  }

  // TODO: Missing implementation;
  virtual bool isDeterministic() const override {
    return true;
  }

  virtual bool isDefaultNullBehavior() const override {
    return false;
  }

  // Builds a new Instance
  static std::shared_ptr<GeneratedVectorFunction<GeneratedCode>> newInstance(
      const std::vector<TypePtr>& inputTypes,
      const std::vector<TypePtr>& outputTypes) {
    return std::make_shared<GeneratedVectorFunction<GeneratedCode>>(
        inputTypes, outputTypes);
  }

 private:
  template <typename... SQLImplType>
  auto toVectorType(const std::tuple<SQLImplType...>&) const {
    return std::vector<TypePtr>{std::make_shared<SQLImplType>()...};
  }

  template <size_t... Indices>
  auto createReaders(
      std::vector<facebook::velox::VectorPtr>& args,
      const std::index_sequence<Indices...>&) const {
    if constexpr (
        GeneratedCode::hasFilter && GeneratedCodeConfig::isDefaultNull) {
      // Filter default null
      return std::make_tuple(
          VectorReader<
              std::tuple_element_t<Indices, InputTypes>,
              InputReaderConfig<indexIn(Indices, FilterInputIndices{}), false>>(
              args[Indices])...);
    } else {
      return std::make_tuple(
          VectorReader<
              std::tuple_element_t<Indices, InputTypes>,
              InputReaderConfig<
                  GeneratedCodeConfig::isDefaultNull,
                  GeneratedCodeConfig::isDefaultNullStrict>>(args[Indices])...);
    }
  }

  template <size_t... Indices>
  auto createWriters(
      std::vector<facebook::velox::VectorPtr>& results,
      const std::index_sequence<Indices...>&) const {
    return std::make_tuple(VectorReader<
                           std::tuple_element_t<Indices, OutputTypes>,
                           OutputReaderConfig<
                               GeneratedCodeConfig::isDefaultNull,
                               GeneratedCodeConfig::isDefaultNullStrict>>(
        results[Indices])...);
  }
  template <typename Func, typename Tuple, size_t... Index>
  inline auto applyToTuple(
      Func&& fun,
      Tuple&& args,
      [[maybe_unused]] const std::index_sequence<Index...>& indices =
          std::index_sequence<Index...>{}) const {
    /// TODO:  This might need to be improved in case func return ref types.
    return std::make_tuple(fun(std::get<Index>(std::forward<Tuple>(args)))...);
  }

  mutable GeneratedCode generatedExpression_;
};

} // namespace codegen
} // namespace velox
} // namespace facebook
