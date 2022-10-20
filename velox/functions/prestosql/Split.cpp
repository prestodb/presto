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

#include "velox/expression/EvalCtx.h"
#include "velox/expression/Expr.h"
#include "velox/expression/StringWriter.h"
#include "velox/expression/VectorFunction.h"
#include "velox/functions/lib/ArrayBuilder.h"
#include "velox/functions/lib/StringEncodingUtils.h"
#include "velox/functions/lib/string/StringImpl.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::functions {

namespace {

/**
 * split(string, delimiter) -> array(varchar)
 *           Splits string on delimiter and returns an array.

 * split(string, delimiter, limit) -> array(varchar)
 *           Splits string on delimiter and returns an array of size at most
 limit.
 * The last element in the array will contain the remainder of the string,
 if such left.
 * limit must be a positive number.
 */

class SplitFunction : public exec::VectorFunction {
 public:
  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    std::vector<std::shared_ptr<exec::FunctionSignature>> signatures;

    // varchar, varchar -> array(varchar)
    signatures.emplace_back(exec::FunctionSignatureBuilder()
                                .returnType("array(varchar)")
                                .argumentType("varchar")
                                .argumentType("varchar")
                                .build());

    // varchar, varchar, integer|bigint -> array(varchar)
    for (const auto& limitType : {"integer", "bigint"}) {
      signatures.emplace_back(exec::FunctionSignatureBuilder()
                                  .returnType("array(varchar)")
                                  .argumentType("varchar")
                                  .argumentType("varchar")
                                  .argumentType(limitType)
                                  .build());
    }
    return signatures;
  }

 private:
  /**
   * The function calls the 'typed' template function based on the type of the
   * 'limit' argument.
   */
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    // If no 'limit' specified we just pick some int type.
    // Should we have two passes, with limit and without?
    TypeKind limitType =
        (args.size() == 2) ? TypeKind::INTEGER : args[2]->typeKind();
    switch (limitType) {
      case TypeKind::INTEGER:
        applyTyped<int32_t>(rows, args, context, result);
        return;
      case TypeKind::BIGINT:
        applyTyped<int64_t>(rows, args, context, result);
        return;
      default:
        VELOX_FAIL(
            "Unsupported type for 'limit' argument of 'split' function: {}",
            mapTypeKindToName(limitType));
    }
  }

  template <typename I>
  void applyTyped(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      exec::EvalCtx& context,
      VectorPtr& result) const {
    // Get the decoded vectors out of arguments.
    const bool noLimit = (args.size() == 2);
    exec::DecodedArgs decodedArgs(rows, args, context);
    DecodedVector* strings = decodedArgs.at(0);
    DecodedVector* delims = decodedArgs.at(1);
    DecodedVector* limits = noLimit ? nullptr : decodedArgs.at(2);

    // Build the result vector (arrays of strings) using Array Builder.
    ArrayBuilder<Varchar> builder(
        rows.end(), 3 * rows.countSelected(), context.pool());

    // Optimization for the (flat, const, const) case.
    if (strings->isIdentityMapping() and delims->isConstantMapping() and
        (noLimit or limits->isConstantMapping())) {
      const auto* rawStrings = strings->data<StringView>();
      const auto delim = delims->valueAt<StringView>(0);

      if (noLimit) {
        const I limit = std::numeric_limits<I>::max();
        rows.applyToSelected([&](vector_size_t row) {
          applyInner<false, I>(rawStrings[row], delim, limit, row, builder);
        });
      } else {
        const I limit = limits->valueAt<I>(0);
        // Limit must be positive.
        if (limit > 0) {
          rows.applyToSelected([&](vector_size_t row) {
            applyInner<true, I>(rawStrings[row], delim, limit, row, builder);
          });
        } else {
          auto pex = std::make_exception_ptr(
              std::invalid_argument("Limit must be positive"));
          context.setErrors(rows, pex);
        }
      }

      // Ensure that our result elements vector uses the same string buffer as
      // the input vector of strings.
      builder.setStringBuffers(
          strings->base()->as<FlatVector<StringView>>()->stringBuffers());
    } else {
      // The rest of the cases are handled through this general path and no
      // direct access.
      applyDecoded<I>(rows, context, strings, delims, limits, builder);

      // Ensure that our result elements vector uses the same string buffer as
      // the input vector of strings.
      builder.setStringBuffers(strings->base());
    }

    std::shared_ptr<ArrayVector> arrayVector =
        std::move(builder).finish(context.pool());
    context.moveOrCopyResult(arrayVector, rows, result);
  }

  template <typename I>
  void applyDecoded(
      const SelectivityVector& rows,
      exec::EvalCtx& context,
      DecodedVector* strings,
      DecodedVector* delims,
      DecodedVector* limits,
      ArrayBuilder<Varchar>& builder) const {
    if (limits == nullptr) {
      auto limit = std::numeric_limits<I>::max();
      rows.applyToSelected([&](vector_size_t row) {
        applyInner<false, I>(
            strings->valueAt<StringView>(row),
            delims->valueAt<StringView>(row),
            limit,
            row,
            builder);
      });
    } else {
      rows.applyToSelected([&](vector_size_t row) {
        const I limit = limits->valueAt<I>(row);
        if (limit > 0) {
          applyInner<true, I>(
              strings->valueAt<StringView>(row),
              delims->valueAt<StringView>(row),
              limit,
              row,
              builder);
        } else {
          auto pex = std::make_exception_ptr(
              std::invalid_argument("Limit must be positive"));
          context.setError(row, pex);
        }
      });
    }
  }

  /**
   * The inner most kernel of the vector operations for 'split'.
   */
  template <bool hasLimit = false, typename I>
  inline void applyInner(
      StringView input,
      const StringView delim,
      I limit,
      vector_size_t row,
      ArrayBuilder<Varchar>& builder) const {
    // Add new array (for the new row) to our array vector.
    auto arrayRef = builder.startArray(row);

    // Trivial case of converting string to array with 1 element.
    if (hasLimit and limit == 1) {
      arrayRef.emplace_back(input);
      return;
    }

    // We walk through our input cutting off the pieces using the delimiter and
    // adding them to the elements vector, until we reached the end of the
    // string or the limit.
    int32_t addedElements{0};
    std::string_view sinput(input.data(), input.size());
    const std::string_view sdelim(delim.data(), delim.size());
    while (true) {
      // Find the byte of the 1st delimiter.
      auto byteIndex = sinput.find(sdelim, 0);

      // Delimiter is not found, leave the loop.
      if (byteIndex == std::string_view::npos) {
        break;
      }

      // Add the new element, we've split
      arrayRef.emplace_back(StringView(sinput.data(), byteIndex));

      // Advance input by the size of the element + delimiter.
      // Note: should we add 'advance' method?
      const auto advanceBytes = byteIndex + sdelim.size();
      sinput = std::string_view(
          sinput.data() + advanceBytes, sinput.size() - advanceBytes);

      if (hasLimit) {
        ++addedElements;
        // If the next element should be the last, leave the loop.
        if (addedElements + 1 == limit) {
          break;
        }
      }
    }

    // Add the rest of the string and we are done.
    // Note, that the rest of the string can be empty - we still add it.
    arrayRef.emplace_back(StringView(sinput.data(), sinput.size()));
  }
};

} // namespace

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_split,
    SplitFunction::signatures(),
    std::make_unique<SplitFunction>());

} // namespace facebook::velox::functions
