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

#include <memory>

#include <velox/expression/DecodedArgs.h>
#include <velox/expression/EvalCtx.h>
#include "velox/common/base/Portability.h"
#include "velox/expression/ComplexWriterTypes.h"
#include "velox/expression/Expr.h"
#include "velox/expression/VectorFunction.h"
#include "velox/expression/VectorReaders.h"
#include "velox/expression/VectorWriters.h"

namespace facebook::velox::exec {

template <typename T>
struct IsArrayWriter {
  static constexpr bool value = false;
};

template <typename T>
struct IsArrayWriter<ArrayWriter<T>> {
  static constexpr bool value = true;
};

template <typename T>
struct IsMapWriter {
  static constexpr bool value = false;
};

template <typename K, typename V>
struct IsMapWriter<MapWriter<K, V>> {
  static constexpr bool value = true;
};

template <typename FUNC>
class SimpleFunctionAdapter : public VectorFunction {
  using T = typename FUNC::exec_return_type;
  using return_type_traits = CppToType<typename FUNC::return_type>;
  template <int32_t POSITION>
  using exec_arg_at = typename std::
      tuple_element<POSITION, typename FUNC::exec_arg_types>::type;
  template <int32_t POSITION>
  using arg_at =
      typename std::tuple_element<POSITION, typename FUNC::arg_types>::type;
  using result_vector_t =
      typename TypeToFlatVector<typename FUNC::return_type>::type;
  std::unique_ptr<FUNC> fn_;

  // Whether the return type for this UDF allows for fast path iteration.
  static constexpr bool fastPathIteration =
      return_type_traits::isPrimitiveType && return_type_traits::isFixedWidth;

  template <int32_t POSITION>
  static constexpr bool isArgFlatConstantFastPathEligible =
      CppToType<arg_at<POSITION>>::typeKind !=
      TypeKind::UNKNOWN&& CppToType<arg_at<POSITION>>::typeKind !=
      TypeKind::BOOLEAN&& CppToType<arg_at<POSITION>>::isPrimitiveType;

  /// If the initialize() method provided by functions throw, we don't (can't)
  /// throw immediately; rather, we capture the exception using this member
  /// variable and set that as error for every single active row. This is needed
  /// because of a subtle semantic issue:
  ///
  /// Consider the function "f(p1, c1)" where c1 is a constant that makes f()
  /// throw on initialize(). If we throw immediately on initialize() and p1 is
  /// composed only of nulls, the expected behavior would be to optimize this
  /// function out and return null, not to throw.
  std::exception_ptr initializeException_;

  struct ApplyContext {
    ApplyContext(
        const SelectivityVector* _rows,
        const TypePtr& outputType,
        EvalCtx* _context,
        VectorPtr* _result,
        bool isResultReused)
        : rows{_rows}, context{_context} {
      // If we're reusing the input, we've already checked that the vector
      // is unique, as is nulls.  We also know the size of the vector is
      // at least as large as the size of rows.
      if (!isResultReused) {
        context->ensureWritable(*rows, outputType, *_result);
      }
      result = reinterpret_cast<result_vector_t*>((*_result).get());
      resultWriter.init(*result);
    }

    template <typename Callable>
    void applyToSelectedNoThrow(Callable func) {
      context->template applyToSelectedNoThrow<Callable>(*rows, func);
    }

    const SelectivityVector* rows;
    result_vector_t* result;
    VectorWriter<typename FUNC::return_type> resultWriter;
    EvalCtx* context;
    bool allAscii{false};
    bool mayHaveNullsRecursive{false};
  };

  template <
      int32_t POSITION,
      typename... Values,
      typename std::enable_if_t<POSITION<FUNC::num_args, int32_t> = 0> void
          unpackInitialize(
              const core::QueryConfig& config,
              const std::vector<VectorPtr>& packed,
              const Values*... values) const {
    if (packed.at(POSITION) != nullptr) {
      SelectivityVector rows(1);
      DecodedVector decodedVector(*packed.at(POSITION), rows);
      auto oneReader = VectorReader<arg_at<POSITION>>(&decodedVector);
      auto oneValue = oneReader[0];

      unpackInitialize<POSITION + 1>(config, packed, values..., &oneValue);
    } else {
      using temp_type = exec_arg_at<POSITION>;
      unpackInitialize<POSITION + 1>(
          config, packed, values..., (const temp_type*)nullptr);
    }
  }

  // unpackInitialize: base case
  template <
      int32_t POSITION,
      typename... Values,
      typename std::enable_if_t<POSITION == FUNC::num_args, int32_t> = 0>
  void unpackInitialize(
      const core::QueryConfig& config,
      const std::vector<VectorPtr>& /*packed*/,
      const Values*... values) const {
    return (*fn_).initialize(config, values...);
  }

 public:
  explicit SimpleFunctionAdapter(
      const core::QueryConfig& config,
      const std::vector<VectorPtr>& constantInputs,
      std::shared_ptr<const Type> returnType)
      : fn_{std::make_unique<FUNC>(move(returnType))} {
    if constexpr (FUNC::udf_has_initialize) {
      try {
        unpackInitialize<0>(config, constantInputs);
      } catch (const std::exception& e) {
        initializeException_ = std::current_exception();
      }
    }
  }

  template <
      int32_t POSITION,
      typename std::enable_if_t<POSITION<FUNC::num_args, int32_t> = 0> bool
          allPrimitiveArgsFlatConstant(const std::vector<VectorPtr>& args)
              const {
    // Variadic args are always last, and we don't support the optimization
    // for them for now.
    if constexpr (isVariadicType<arg_at<POSITION>>::value) {
      return true;
    } else if constexpr (isArgFlatConstantFastPathEligible<POSITION>) {
      if (!args[POSITION]->isFlatEncoding() &&
          !args[POSITION]->isConstantEncoding()) {
        return false;
      }
    }

    return allPrimitiveArgsFlatConstant<POSITION + 1>(args);
  }

  template <
      int32_t POSITION,
      typename std::enable_if_t<POSITION == FUNC::num_args, int32_t> = 0>
  bool allPrimitiveArgsFlatConstant(
      const std::vector<VectorPtr>& /*args*/) const {
    // Base case.
    return true;
  }

  template <
      int32_t POSITION,
      typename std::enable_if_t<POSITION<FUNC::num_args, int32_t> = 0> void
          decodeArgs(
              std::vector<LocalDecodedVector>& decodedArgs,
              const std::vector<VectorPtr>& args,
              const SelectivityVector& rows,
              EvalCtx* context,
              bool decodePrimitives) const {
    if constexpr (isVariadicType<arg_at<POSITION>>::value) {
      // Decode the underlying arguments of the Variadic type.
      for (int i = POSITION; i < args.size(); ++i) {
        decodedArgs.emplace_back(context, *args[i], rows);
      }
    } else if constexpr (isArgFlatConstantFastPathEligible<POSITION>) {
      if (decodePrimitives) {
        decodedArgs.emplace_back(context, *args[POSITION], rows);
      } else {
        // If we're skipping decoding this argument, add a dummy value.
        decodedArgs.emplace_back(context);
      }
    } else {
      decodedArgs.emplace_back(context, *args[POSITION], rows);
    }

    decodeArgs<POSITION + 1>(
        decodedArgs, args, rows, context, decodePrimitives);
  }

  template <
      int32_t POSITION,
      typename std::enable_if_t<POSITION == FUNC::num_args, int32_t> = 0>
  void decodeArgs(
      std::vector<LocalDecodedVector>& /*decodedArgs*/,
      const std::vector<VectorPtr>& /*args*/,
      const SelectivityVector& /*rows*/,
      EvalCtx* /*context*/,
      bool /*decodePrimitives*/) const {
    // No-op base case.
  }

  template <
      int32_t POSITION,
      typename std::enable_if_t<POSITION<FUNC::num_args, int32_t> = 0>
          VectorPtr* findReusableArg(std::vector<VectorPtr>& args) const {
    if constexpr (isVariadicType<arg_at<POSITION>>::value) {
      if constexpr (
          CppToType<typename arg_at<POSITION>::underlying_type>::typeKind ==
          return_type_traits::typeKind) {
        for (auto i = POSITION; i < args.size(); i++) {
          if (BaseVector::isReusableFlatVector(args[i])) {
            return &args[i];
          }
        }
      }
      // A Variadic arg is always the last, so if we haven't found a match yet,
      // we know for sure that we won't.
      return nullptr;
    } else if constexpr (
        CppToType<arg_at<POSITION>>::typeKind == return_type_traits::typeKind) {
      if (BaseVector::isReusableFlatVector(args[POSITION])) {
        // Re-use arg for result. We rely on the fact that for each row
        // we read arguments before computing and writing out the
        // result.
        return &args[POSITION];
      }
    }

    return findReusableArg<POSITION + 1>(args);
  }

  template <
      int32_t POSITION,
      typename std::enable_if_t<POSITION == FUNC::num_args, int32_t> = 0>
  VectorPtr* findReusableArg(std::vector<VectorPtr>& args) const {
    // Base case: we didn't find an input vector to reuse.
    return nullptr;
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      EvalCtx* context,
      VectorPtr* result) const override {
    auto* reusableResult = result;
    // If result is null, check if one of the arguments can be re-used for
    // storing the result. This is possible if all the following conditions are
    // met:
    // - result type is a fixed-width type,
    // - function doesn't produce nulls,
    // - an argument has the same type as result,
    // - the argument has flat encoding,
    // - the argument is singly-referenced and has singly-referenced values
    // and nulls buffers.
    bool isResultReused = false;
    if constexpr (
        !FUNC::can_produce_null_output && !FUNC::udf_has_callNullFree &&
        return_type_traits::isPrimitiveType &&
        return_type_traits::isFixedWidth) {
      if (!reusableResult->get()) {
        if (auto arg = findReusableArg<0>(args)) {
          reusableResult = arg;
          isResultReused = true;
        }
      }
    }

    ApplyContext applyContext{
        &rows, outputType, context, reusableResult, isResultReused};

    // If the function provides an initialize() method and it threw, we set that
    // exception in all active rows and we're done with it.
    if constexpr (FUNC::udf_has_initialize) {
      if (UNLIKELY(initializeException_ != nullptr)) {
        applyContext.context->setErrors(
            *applyContext.rows, initializeException_);
        return;
      }
    }

    // Enable fast all-ASCII path if all string inputs are ASCII and the
    // function provides ASCII-only path.
    if constexpr (FUNC::has_ascii) {
      applyContext.allAscii = isAsciiArgs(rows, args);
    }

    // If this UDF can take the fast path iteration, we set all active rows as
    // non-nulls in the result vector. The assumption is that the majority of
    // rows will return non-null values (and hence won't have to touch the
    // null buffer during iteration).
    // If this function doesn't produce nulls, then one of its arguments may be
    // used for storing results. In this case, do not clear the nulls before
    // processing these.
    if constexpr (
        fastPathIteration &&
        (FUNC::can_produce_null_output || FUNC::udf_has_callNullFree)) {
      (*reusableResult)->clearNulls(rows);
    }

    bool primitiveFlatConstantFastPath = allPrimitiveArgsFlatConstant<0>(args);

    std::vector<LocalDecodedVector> decoded;
    decoded.reserve(args.size());
    decodeArgs<0>(decoded, args, rows, context, !primitiveFlatConstantFastPath);

    if (primitiveFlatConstantFastPath) {
      unpack<0, true>(applyContext, true, decoded, args);
    } else {
      unpack<0, false>(applyContext, true, decoded, args);
    }

    if constexpr (
        fastPathIteration && !FUNC::can_produce_null_output &&
        !FUNC::udf_has_callNullFree) {
      (*reusableResult)->clearNulls(rows);
    }

    // Check if the function reuses input strings for the result, and add
    // references to input string buffers to all result vectors.
    auto reuseStringsFromArg = fn_->reuseStringsFromArg();
    if (reuseStringsFromArg >= 0) {
      VELOX_CHECK_LT(reuseStringsFromArg, args.size());
      VELOX_CHECK_EQ(args[reuseStringsFromArg]->typeKind(), TypeKind::VARCHAR);
      if (primitiveFlatConstantFastPath) {
        // primitiveFlatConstantFastPath is only true if all primitives are
        // encoded in Flat or Constant Vectors.  Since varchar is a primitive
        // type, if we're here, we're guaranteed the argument is either a Flat
        // or Constant vector so no decoding is necessary.
        tryAcquireStringBuffer(
            reusableResult->get(), args.at(reuseStringsFromArg).get());
      } else {
        tryAcquireStringBuffer(
            reusableResult->get(),
            decoded.at(reuseStringsFromArg).get()->base());
      }
    }

    *result = std::move(*reusableResult);
  }

  // Acquire string buffer from source if vector is a string flat vector.
  void tryAcquireStringBuffer(BaseVector* vector, const BaseVector* source)
      const {
    if (auto* flatVector = vector->asFlatVector<StringView>()) {
      flatVector->acquireSharedStringBuffers(source);
    } else if (auto* arrayVector = vector->as<ArrayVector>()) {
      tryAcquireStringBuffer(arrayVector->elements().get(), source);
    } else if (auto* mapVector = vector->as<MapVector>()) {
      tryAcquireStringBuffer(mapVector->mapKeys().get(), source);
      tryAcquireStringBuffer(mapVector->mapValues().get(), source);
    } else if (auto* rowVector = vector->as<RowVector>()) {
      for (auto& it : rowVector->children()) {
        tryAcquireStringBuffer(it.get(), source);
      }
    }
  }

  bool isDeterministic() const override {
    return fn_->isDeterministic();
  }

  bool isDefaultNullBehavior() const override {
    return fn_->is_default_null_behavior;
  }

  bool supportsFlatNoNullsFastPath() const override {
    return !FUNC::can_produce_null_output;
  }

  bool ensureStringEncodingSetAtAllInputs() const override {
    return fn_->has_ascii;
  }

  bool propagateStringEncodingFromAllInputs() const override {
    return fn_->is_default_ascii_behavior;
  }

 private:
  /// Return true if at least one argument has type VARCHAR and all VARCHAR
  /// arguments are all-ASCII for the specified rows.
  static bool isAsciiArgs(
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args) {
    bool hasStringArgs = false;
    bool allAscii = true;
    for (auto& arg : args) {
      if (arg->type()->isVarchar()) {
        hasStringArgs = true;
        auto stringArg = arg->asUnchecked<SimpleVector<StringView>>();
        if (auto isAscii = stringArg->isAscii(rows)) {
          if (!isAscii.value()) {
            allAscii = false;
            break;
          }
        }
      }
    }

    return hasStringArgs && allAscii;
  }

  template <
      int32_t POSITION,
      bool primitiveFlatConstantFastPath,
      typename... TReader,
      typename std::enable_if_t<POSITION<FUNC::num_args, int32_t> = 0> void
          unpack(
              ApplyContext& applyContext,
              bool allNotNull,
              std::vector<LocalDecodedVector>& decodedArgs,
              const std::vector<VectorPtr>& rawArgs,
              TReader&... readers) const {
    if constexpr (isVariadicType<arg_at<POSITION>>::value) {
      // This should already be statically checked by the UDFHolder used to
      // wrap the simple function, but checking again here just in case.
      static_assert(
          POSITION == FUNC::num_args - 1,
          "Variadic args can only be used as the last argument to a function.");
      auto oneReader = VectorReader<arg_at<POSITION>>(decodedArgs, POSITION);

      if constexpr (FUNC::udf_has_callNullFree) {
        oneReader.setChildrenMayHaveNulls();
        applyContext.mayHaveNullsRecursive |= oneReader.mayHaveNullsRecursive();
      }

      bool nextNonNull = applyContext.context->nullsPruned();
      if (!nextNonNull && allNotNull) {
        nextNonNull = true;
        for (auto i = POSITION; i < decodedArgs.size(); i++) {
          nextNonNull &= !decodedArgs.at(i).get()->mayHaveNulls();
        }
      }

      unpack<POSITION + 1, primitiveFlatConstantFastPath>(
          applyContext,
          nextNonNull,
          decodedArgs,
          rawArgs,
          readers...,
          oneReader);
    } else {
      if constexpr (
          CppToType<arg_at<POSITION>>::isPrimitiveType &&
          CppToType<arg_at<POSITION>>::typeKind != TypeKind::UNKNOWN &&
          CppToType<arg_at<POSITION>>::typeKind != TypeKind::BOOLEAN &&
          primitiveFlatConstantFastPath) {
        using value_t =
            typename ConstantFlatVectorReader<arg_at<POSITION>>::exec_in_t;
        auto& arg = rawArgs[POSITION];
        auto oneReader = arg->encoding() == VectorEncoding::Simple::FLAT
            ? ConstantFlatVectorReader<arg_at<POSITION>>(
                  static_cast<FlatVector<value_t>*>(arg.get()))
            : ConstantFlatVectorReader<arg_at<POSITION>>(
                  static_cast<ConstantVector<value_t>*>(arg.get()));

        if constexpr (FUNC::udf_has_callNullFree) {
          oneReader.setChildrenMayHaveNulls();
          applyContext.mayHaveNullsRecursive |=
              oneReader.mayHaveNullsRecursive();
        }

        // context->nullPruned() is true after rows with nulls have been
        // pruned out of 'rows', so we won't be seeing any more nulls
        // here.
        bool nextNonNull = applyContext.context->nullsPruned() ||
            (allNotNull && !arg->mayHaveNulls());

        unpack<POSITION + 1, primitiveFlatConstantFastPath>(
            applyContext,
            nextNonNull,
            decodedArgs,
            rawArgs,
            readers...,
            oneReader);
      } else {
        auto* oneUnpacked = decodedArgs.at(POSITION).get();
        auto oneReader = VectorReader<arg_at<POSITION>>(oneUnpacked);

        if constexpr (FUNC::udf_has_callNullFree) {
          oneReader.setChildrenMayHaveNulls();
          applyContext.mayHaveNullsRecursive |=
              oneReader.mayHaveNullsRecursive();
        }

        // context->nullPruned() is true after rows with nulls have been
        // pruned out of 'rows', so we won't be seeing any more nulls here.
        bool nextNonNull = applyContext.context->nullsPruned() ||
            (allNotNull && !oneUnpacked->mayHaveNulls());

        unpack<POSITION + 1, primitiveFlatConstantFastPath>(
            applyContext,
            nextNonNull,
            decodedArgs,
            rawArgs,
            readers...,
            oneReader);
      }
    }
  }

  // unpacking zips like const char* notnull, const T* values

  // unpack: base case
  template <
      int32_t POSITION,
      bool primitiveFlatConstantFastPath,
      typename... TReader,
      typename std::enable_if_t<POSITION == FUNC::num_args, int32_t> = 0>
  void unpack(
      ApplyContext& applyContext,
      bool allNotNull,
      std::vector<LocalDecodedVector>& /*decodedArgs*/,
      const std::vector<VectorPtr>& /*rawArgs*/,
      const TReader&... readers) const {
    iterate(applyContext, allNotNull, readers...);
  }

  template <typename... TReader>
  void iterate(
      ApplyContext& applyContext,
      bool allNotNull,
      const TReader&... readers) const {
    // If is_default_contains_nulls_behavior we return null if the inputs
    // contain any nulls.
    // If !is_default_contains_nulls_behavior we don't invoke callNullFree
    // if the inputs contain any nulls, but rather invoke call or
    // callNullable as usual.
    bool callNullFree = FUNC::is_default_contains_nulls_behavior ||
        (FUNC::udf_has_callNullFree && !applyContext.mayHaveNullsRecursive);

    // Iterate the rows.
    if constexpr (fastPathIteration) {
      uint64_t* nullBuffer = nullptr;
      auto* data = applyContext.result->mutableRawValues();
      auto writeResult = [&applyContext, &nullBuffer, &data](
                             auto row, bool notNull, auto out) INLINE_LAMBDA {
        // For fast path iteration, all active rows were already set as
        // non-null beforehand, so we only need to update the null buffer if
        // the function returned null (which is not the common case).
        if (notNull) {
          if constexpr (return_type_traits::typeKind == TypeKind::BOOLEAN) {
            bits::setBit(data, row, out);
          } else {
            data[row] = out;
          }
        } else {
          if (!nullBuffer) {
            nullBuffer = applyContext.result->mutableRawNulls();
          }
          bits::setNull(nullBuffer, row);
        }
      };
      if (callNullFree) {
        // This results in some code duplication, but applying this check
        // once per batch instead of once per row shows a significant
        // performance improvement when there are no nulls.
        if (applyContext.mayHaveNullsRecursive) {
          applyContext.applyToSelectedNoThrow([&](auto row) INLINE_LAMBDA {
            typename return_type_traits::NativeType out{};
            auto containsNull = (readers.containsNull(row) || ...);
            bool notNull;
            if (containsNull) {
              // Result is NULL because the input contains NULL.
              notNull = false;
            } else {
              notNull = doApplyNullFree<0>(row, out, readers...);
            }

            writeResult(row, notNull, out);
          });
        } else {
          applyContext.applyToSelectedNoThrow([&](auto row) INLINE_LAMBDA {
            typename return_type_traits::NativeType out{};
            bool notNull = doApplyNullFree<0>(row, out, readers...);

            writeResult(row, notNull, out);
          });
        }
      } else if (allNotNull) {
        applyContext.applyToSelectedNoThrow([&](auto row) INLINE_LAMBDA {
          // Passing a stack variable have shown to be boost the performance
          // of functions that repeatedly update the output. The opposite
          // optimization (eliminating the temp) is easier to do by the
          // compiler (assuming the function call is inlined).
          typename return_type_traits::NativeType out{};
          bool notNull = doApplyNotNull<0>(row, out, readers...);
          writeResult(row, notNull, out);
        });
      } else {
        applyContext.applyToSelectedNoThrow([&](auto row) INLINE_LAMBDA {
          typename return_type_traits::NativeType out{};
          bool notNull = doApply<0>(row, out, readers...);
          writeResult(row, notNull, out);
        });
      }
    } else {
      if (callNullFree) {
        // This results in some code duplication, but applying this check
        // once per batch instead of once per row shows a significant
        // performance improvement when there are no nulls.
        if (applyContext.mayHaveNullsRecursive) {
          applyUdf(applyContext, [&](auto& out, auto row) INLINE_LAMBDA {
            auto containsNull = (readers.containsNull(row) || ...);
            if (containsNull) {
              // Result is NULL because the input contains NULL.
              return false;
            }

            return doApplyNullFree<0>(row, out, readers...);
          });
        } else {
          applyUdf(applyContext, [&](auto& out, auto row) INLINE_LAMBDA {
            return doApplyNullFree<0>(row, out, readers...);
          });
        }
      } else if (allNotNull) {
        if (applyContext.allAscii) {
          applyUdf(applyContext, [&](auto& out, auto row) INLINE_LAMBDA {
            return doApplyAsciiNotNull<0>(row, out, readers...);
          });
        } else {
          applyUdf(applyContext, [&](auto& out, auto row) INLINE_LAMBDA {
            return doApplyNotNull<0>(row, out, readers...);
          });
        }
      } else {
        applyUdf(applyContext, [&](auto& out, auto row) INLINE_LAMBDA {
          return doApply<0>(row, out, readers...);
        });
      }
    }
  }

  template <typename Func>
  void applyUdf(ApplyContext& applyContext, Func func) const {
    if constexpr (IsArrayWriter<T>::value || IsMapWriter<T>::value) {
      // An optimization for arrayProxy and mapWriter that force the
      // localization of the writer.
      auto& currentWriter = applyContext.resultWriter.writer_;

      applyContext.applyToSelectedNoThrow([&](auto row) INLINE_LAMBDA {
        applyContext.resultWriter.setOffset(row);
        // Force local copy of proxy.
        auto localWriter = currentWriter;
        auto notNull = func(localWriter, row);
        currentWriter = localWriter;
        applyContext.resultWriter.commit(notNull);
      });
      applyContext.resultWriter.finish();
    } else {
      applyContext.applyToSelectedNoThrow([&](auto row) INLINE_LAMBDA {
        applyContext.resultWriter.setOffset(row);
        applyContext.resultWriter.commit(
            func(applyContext.resultWriter.current(), row));
      });
    }
  }

  // == NULLABLE VARIANTS ==

  // For default null behavior, assume everything is not null.
  // If anything is, return false.
  // Otherwise pass all arguments as references or copies.
  template <
      size_t POSITION,
      typename R0,
      typename... Values,
      std::enable_if_t<
          POSITION<FUNC::num_args && FUNC::is_default_null_behavior, int32_t> =
              0> FOLLY_ALWAYS_INLINE bool
          doApply(
              size_t idx,
              T& target,
              R0& currentReader,
              const Values&... extra) const {
    if (LIKELY(currentReader.isSet(idx))) {
      // decltype for reference if appropriate, otherwise copy
      decltype(currentReader[idx]) v0 = currentReader[idx];

      // recurse through the readers to build the arg list at compile time.
      return doApply<POSITION + 1>(idx, target, extra..., v0);
    } else {
      return false;
    }
  }

  // For NOT default null behavior get pointers.
  template <
      size_t POSITION,
      typename R0,
      typename... Values,
      std::enable_if_t<
          POSITION<FUNC::num_args && !FUNC::is_default_null_behavior, int32_t> =
              0> FOLLY_ALWAYS_INLINE bool
          doApply(
              size_t idx,
              T& target,
              R0& currentReader,
              const Values&... extra) const {
    // Recurse through all the arguments to build the arg list at compile
    // time.
    if constexpr (std::is_reference<decltype(currentReader[idx])>::value) {
      return doApply<POSITION + 1>(
          idx,
          target,
          extra...,
          (currentReader.isSet(idx) ? &currentReader[idx] : nullptr));
    } else {
      using temp_type = std::remove_reference_t<decltype(currentReader[idx])>;
      if (currentReader.isSet(idx)) {
        temp_type temp = currentReader[idx];
        return doApply<POSITION + 1>(idx, target, extra..., &temp);
      } else {
        return doApply<POSITION + 1>(
            idx, target, extra..., (const temp_type*)nullptr);
      }
    }
  }

  // For default null behavior, Terminate by with UDFHolder::call.
  template <
      size_t POSITION,
      typename... Values,
      std::enable_if_t<
          POSITION == FUNC::num_args && FUNC::is_default_null_behavior,
          int32_t> = 0>
  FOLLY_ALWAYS_INLINE bool
  doApply(size_t /*idx*/, T& target, const Values&... values) const {
    return (*fn_).call(target, values...);
  }

  // For NOT default null behavior, terminate with UDFHolder::callNullable.
  template <
      size_t POSITION,
      typename... Values,
      std::enable_if_t<
          POSITION == FUNC::num_args && !FUNC::is_default_null_behavior,
          int32_t> = 0>
  FOLLY_ALWAYS_INLINE bool
  doApply(size_t /*idx*/, T& target, const Values*... values) const {
    return (*fn_).callNullable(target, values...);
  }

  // == NOT-NULL VARIANT ==

  // If we're guaranteed not to have any nulls, pass all parameters as
  // references.
  //
  // Note that (*fn_).call() will internally dispatch the call to either
  // call() or callNullable() (whichever is implemented by the user
  // function). Default null behavior or not does not matter in this path
  // since we don't have any nulls.
  template <
      size_t POSITION,
      typename R0,
      typename... TStuff,
      std::enable_if_t<POSITION != FUNC::num_args, int32_t> = 0>
  FOLLY_ALWAYS_INLINE bool doApplyNotNull(
      size_t idx,
      T& target,
      R0& currentReader,
      const TStuff&... extra) const {
    decltype(currentReader[idx]) v0 = currentReader[idx];
    return doApplyNotNull<POSITION + 1>(idx, target, extra..., v0);
  }

  // For default null behavior, Terminate by with UDFHolder::call.
  template <
      size_t POSITION,
      typename... Values,
      std::enable_if_t<POSITION == FUNC::num_args, int32_t> = 0>
  FOLLY_ALWAYS_INLINE bool
  doApplyNotNull(size_t /*idx*/, T& target, const Values&... values) const {
    return (*fn_).call(target, values...);
  }

  template <
      size_t POSITION,
      typename R0,
      typename... TStuff,
      std::enable_if_t<POSITION != FUNC::num_args, int32_t> = 0>
  FOLLY_ALWAYS_INLINE bool doApplyAsciiNotNull(
      size_t idx,
      T& target,
      R0& currentReader,
      const TStuff&... extra) const {
    decltype(currentReader[idx]) v0 = currentReader[idx];
    return doApplyAsciiNotNull<POSITION + 1>(idx, target, extra..., v0);
  }

  template <
      size_t POSITION,
      typename... Values,
      std::enable_if_t<POSITION == FUNC::num_args, int32_t> = 0>
  FOLLY_ALWAYS_INLINE bool doApplyAsciiNotNull(
      size_t /*idx*/,
      T& target,
      const Values&... values) const {
    return (*fn_).callAscii(target, values...);
  }

  template <
      size_t POSITION,
      typename R0,
      typename... TStuff,
      std::enable_if_t<POSITION != FUNC::num_args, int32_t> = 0>
  FOLLY_ALWAYS_INLINE bool doApplyNullFree(
      size_t idx,
      T& target,
      R0& currentReader,
      const TStuff&... extra) const {
    auto v0 = currentReader.readNullFree(idx);
    return doApplyNullFree<POSITION + 1>(idx, target, extra..., v0);
  }

  template <
      size_t POSITION,
      typename... Values,
      std::enable_if_t<POSITION == FUNC::num_args, int32_t> = 0>
  FOLLY_ALWAYS_INLINE bool
  doApplyNullFree(size_t /*idx*/, T& target, const Values&... values) const {
    return (*fn_).callNullFree(target, values...);
  }
};

template <typename UDFHolder>
class SimpleFunctionAdapterFactoryImpl : public SimpleFunctionAdapterFactory {
 public:
  // Exposed for use in FunctionRegistry
  using Metadata = typename UDFHolder::Metadata;

  explicit SimpleFunctionAdapterFactoryImpl(
      std::shared_ptr<const Type> returnType)
      : returnType_(std::move(returnType)) {}

  std::unique_ptr<VectorFunction> createVectorFunction(
      const core::QueryConfig& config,
      const std::vector<VectorPtr>& constantInputs) const override {
    return std::make_unique<SimpleFunctionAdapter<UDFHolder>>(
        config, constantInputs, returnType_);
  }

 private:
  const std::shared_ptr<const Type> returnType_;
};

} // namespace facebook::velox::exec
