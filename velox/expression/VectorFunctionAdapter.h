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

#include "velox/expression/Expr.h"
#include "velox/expression/VectorFunction.h"
#include "velox/expression/VectorUdfTypeSystem.h"
#include "velox/vector/VectorTypeUtils.h"

namespace facebook::velox::exec {

template <typename FUNC>
class VectorAdapter : public VectorFunction {
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

  struct ApplyContext {
    ApplyContext(
        const SelectivityVector* _rows,
        Expr* caller,
        EvalCtx* _context,
        VectorPtr* _result)
        : rows{_rows}, context{_context} {
      BaseVector::ensureWritable(
          *rows, caller->type(), context->pool(), _result);
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
  };

 public:
  explicit VectorAdapter(
      const core::QueryConfig& config,
      std::shared_ptr<const Type> returnType)
      : fn_{std::make_unique<FUNC>(move(returnType))} {
    fn_->initialize(config);
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      Expr* caller,
      EvalCtx* context,
      VectorPtr* result) const override {
    ApplyContext applyContext{&rows, caller, context, result};
    DecodedArgs decodedArgs{rows, args, context};

    // Enable fast all-ASCII path if all string inputs are ASCII and the
    // function provides ASCII-only path.
    if constexpr (FUNC::has_ascii) {
      applyContext.allAscii = isAsciiArgs(rows, args);
    }

    unpack<0>(applyContext, true, decodedArgs);

    // Check if the function reuses input strings for the result and add
    // references to input string buffers to result vector.
    auto reuseStringsFromArg = fn_->reuseStringsFromArg();
    if (reuseStringsFromArg >= 0) {
      VELOX_CHECK_EQ((*result)->typeKind(), TypeKind::VARCHAR);
      VELOX_CHECK_LT(reuseStringsFromArg, args.size());
      VELOX_CHECK_EQ(args[reuseStringsFromArg]->typeKind(), TypeKind::VARCHAR);

      (*result)->as<FlatVector<StringView>>()->acquireSharedStringBuffers(
          decodedArgs.at(reuseStringsFromArg)->base());
    }
  }

  bool isDeterministic() const override {
    return fn_->isDeterministic();
  }

  bool isDefaultNullBehavior() const override {
    return fn_->is_default_null_behavior;
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
      typename... TReader,
      typename std::enable_if_t<POSITION<FUNC::num_args, int32_t> = 0> void
          unpack(
              ApplyContext& applyContext,
              bool allNotNull,
              const DecodedArgs& packed,
              TReader&... readers) const {
    auto oneUnpacked = packed.at(POSITION)->as<exec_arg_at<POSITION>>();
    auto oneReader = VectorReader<arg_at<POSITION>>(oneUnpacked);

    // context->nullPruned() is true after rows with nulls have been
    // pruned out of 'rows', so we won't be seeing any more nulls here.
    bool nextNonNull = applyContext.context->nullsPruned() ||
        (allNotNull && !oneUnpacked.mayHaveNulls());
    unpack<POSITION + 1>(
        applyContext, nextNonNull, packed, readers..., oneReader);
  }

  // unpacking zips like const char* notnull, const T* values

  // todo(youknowjack): I don't think this will work with more than 2 arguments
  //                    how can compiler know how to expand the packs?

  // unpack: base case
  template <
      int32_t POSITION,
      typename... TReader,
      typename std::enable_if_t<POSITION == FUNC::num_args, int32_t> = 0>
  void unpack(
      ApplyContext& applyContext,
      bool allNotNull,
      const DecodedArgs& /*packed*/,
      const TReader&... readers) const {
    iterate(applyContext, allNotNull, readers...);
  }

  template <typename... TReader>
  void iterate(
      ApplyContext& applyContext,
      bool allNotNull,
      const TReader&... readers) const {
    // iterate the rows
    if constexpr (
        return_type_traits::isPrimitiveType &&
        return_type_traits::isFixedWidth &&
        return_type_traits::typeKind != TypeKind::BOOLEAN) {
      // "writer" gets in the way for primitives, so we specialize
      uint64_t* nn = nullptr;
      auto* data = applyContext.result->mutableRawValues();
      if (allNotNull) {
        applyContext.applyToSelectedNoThrow([&](auto row) {
          bool notNull = doApplyNotNull<0>(row, data[row], readers...);
          if (!notNull) {
            if (!nn) {
              nn = applyContext.result->mutableRawNulls();
            }
            bits::setNull(nn, row);
          }
        });
      } else {
        applyContext.applyToSelectedNoThrow([&](auto row) {
          bool notNull = doApply<0>(row, data[row], readers...);
          if (!notNull) {
            if (!nn) {
              nn = applyContext.result->mutableRawNulls();
            }
            bits::setNull(nn, row);
          }
        });
      }
    } else {
      if (allNotNull) {
        if (applyContext.allAscii) {
          applyContext.applyToSelectedNoThrow([&](auto row) {
            applyContext.resultWriter.setOffset(row);
            applyContext.resultWriter.commit(
                static_cast<char>(doApplyAsciiNotNull<0>(
                    row, applyContext.resultWriter.current(), readers...)));
          });
        } else {
          applyContext.applyToSelectedNoThrow([&](auto row) {
            applyContext.resultWriter.setOffset(row);
            applyContext.resultWriter.commit(
                static_cast<char>(doApplyNotNull<0>(
                    row, applyContext.resultWriter.current(), readers...)));
          });
        }
      } else {
        applyContext.applyToSelectedNoThrow([&](auto row) {
          applyContext.resultWriter.setOffset(row);
          applyContext.resultWriter.commit(static_cast<char>(doApply<0>(
              row, applyContext.resultWriter.current(), readers...)));
        });
      }
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
    // Recurse through all the arguments to build the arg list at compile time.
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

  // For default null behavior, Terminate by with ScalarFunction::call.
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

  // For NOT default null behavior, terminate with ScalarFunction::callNullable.
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
  // Note that (*fn_).call() will internally dispatch the call to either call()
  // or callNullable() (whichever is implemented by the user function). Default
  // null behavior or not does not matter in this path since we don't have any
  // nulls.
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

  // For default null behavior, Terminate by with ScalarFunction::call.
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
};

template <typename FUNC>
class VectorAdapterFactoryImpl : public VectorAdapterFactory {
 public:
  explicit VectorAdapterFactoryImpl(std::shared_ptr<const Type> returnType)
      : returnType_(std::move(returnType)) {}

  std::unique_ptr<VectorFunction> getVectorInterpreter(
      const core::QueryConfig& config) const override {
    return std::make_unique<VectorAdapter<FUNC>>(config, returnType_);
  }

  const std::shared_ptr<const Type> returnType() const override {
    return returnType_;
  }

  bool isDeterministic() {
    return FUNC::isDeterministic;
  }

  bool isDefaultNullBehavior() {
    return FUNC::is_default_null_behavior;
  }

 private:
  const std::shared_ptr<const Type> returnType_;
};

} // namespace facebook::velox::exec
