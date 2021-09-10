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
#include "velox/expression/VectorFunction.h"
#include "velox/expression/VectorUdfTypeSystem.h"
#include "velox/functions/lib/StringEncodingUtils.h"
#include "velox/functions/lib/string/StringImpl.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::functions {
namespace {

/// Check if the input vector's  buffers are single referenced
bool hasSingleReferencedBuffers(const FlatVector<StringView>* vec) {
  for (auto& buffer : vec->getStringBuffers()) {
    if (buffer->refCount() > 1) {
      return false;
    }
  }
  return true;
};

/// Helper function that prepares a string result vector and initializes it.
/// It will use the input argToReuse vector instead of creating new one when
/// possible. Returns true if argToReuse vector was moved to results
VectorPtr emptyVectorPtr;
bool prepareFlatResultsVector(
    VectorPtr* result,
    const SelectivityVector& rows,
    exec::EvalCtx* context,
    VectorPtr& argToReuse = emptyVectorPtr) {
  if (!*result && argToReuse && argToReuse.unique()) {
    // Move input vector to result
    VELOX_CHECK(
        argToReuse.get()->encoding() == VectorEncoding::Simple::FLAT &&
        argToReuse.get()->typeKind() == TypeKind::VARCHAR);

    *result = std::move(argToReuse);
    return true;
  }
  // This will allocate results if not allocated
  BaseVector::ensureWritable(rows, VARCHAR(), context->pool(), result);

  VELOX_CHECK((*result).get()->encoding() == VectorEncoding::Simple::FLAT);
  return false;
}

/**
 * substr(string, start) -> varchar
 *           Returns the rest of string from the starting position start.
 * Positions start with 1. A negative starting position is interpreted as being
 * relative to the end of the string.

 * substr(string, start, length) -> varchar
 *           Returns a substring from string of length length from the starting
 * position start. Positions start with 1. A negative starting position is
 * interpreted as being relative to the end of the string.
 */

class SubstrFunction : public exec::VectorFunction {
 public:
  bool isDefaultNullBehavior() const override {
    return true;
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    std::vector<std::shared_ptr<exec::FunctionSignature>> signatures;

    // varchar, integer|bigint -> varchar
    for (const auto& startType : {"integer", "bigint"}) {
      signatures.emplace_back(exec::FunctionSignatureBuilder()
                                  .returnType("varchar")
                                  .argumentType("varchar")
                                  .argumentType(startType)
                                  .build());
    }

    // varchar, integer|bigint, integer|bigint -> varchar
    for (const auto& startType : {"integer", "bigint"}) {
      for (const auto& lengthType : {"integer", "bigint"}) {
        signatures.emplace_back(exec::FunctionSignatureBuilder()
                                    .returnType("varchar")
                                    .argumentType("varchar")
                                    .argumentType(startType)
                                    .argumentType(lengthType)
                                    .build());
      }
    }
    return signatures;
  }

 private:
  /**
   * The inner most kernel of the vector operations for substr
   */
  template <typename I>
  inline void applyInner(
      I start,
      I length,
      const StringView& input,
      StringView& output,
      bool isAscii) const {
    // Following Presto semantics
    if (start == 0) {
      output = StringView("");
      return;
    }

    I numCharacters;
    if (isAscii) {
      numCharacters = stringImpl::length</*isAscii*/ true>(input);
    } else {
      numCharacters = stringImpl::length</*isAscii*/ false>(input);
    }

    // Adjusting start
    if (start < 0) {
      start = numCharacters + start + 1;
    }

    // Following Presto semantics
    if (start <= 0 || start > numCharacters || length <= 0) {
      output = StringView("");
      return;
    }

    // Adjusting length
    if (length == std::numeric_limits<I>::max() ||
        length + start - 1 > numCharacters) {
      // set length to the max valid length
      length = numCharacters - start + 1;
    }

    std::pair<size_t, size_t> byteRange;
    if (isAscii) {
      byteRange = getByteRange</*isAscii*/ true>(input.data(), start, length);
    } else {
      byteRange = getByteRange</*isAscii*/ false>(input.data(), start, length);
    }

    // Generating output string
    output = StringView(
        input.data() + byteRange.first, byteRange.second - byteRange.first);
  }

  /**
   * The function calls the right typed function based on the vector types
   */
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      exec::Expr* caller,
      exec::EvalCtx* context,
      VectorPtr* result) const override {
    VELOX_CHECK(args.size() == 3 || args.size() == 2);
    if (args.size() == 3) {
      VELOX_CHECK(args[1]->typeKind() == args[2]->typeKind());
    }
    switch (args[1]->typeKind()) {
      case TypeKind::INTEGER:
        applyTyped<int32_t>(rows, args, caller, context, result);
        return;
      case TypeKind::BIGINT:
        applyTyped<int64_t>(rows, args, caller, context, result);
        return;
      default:
        VELOX_CHECK(false, "Bad type for substr");
    }
  }

  template <typename I>
  void applyTyped(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      exec::Expr* caller,
      exec::EvalCtx* context,
      VectorPtr* result) const {
    // We use this flag to enable the version of the function in which length is
    // not provided.
    bool noLengthVector = (args.size() == 2);
    BaseVector* stringsVector = args[0].get();
    BaseVector* startsVector = args[1].get();
    BaseVector* lengthsVector = noLengthVector ? nullptr : args[2].get();
    auto ascii = isAscii(stringsVector, rows);

    auto stringArgVectorEncoding = stringsVector->encoding();
    auto startArgVectorEncoding = startsVector->encoding();
    auto lengthArgVectorEncoding =
        noLengthVector ? startArgVectorEncoding : lengthsVector->encoding();

    prepareFlatResultsVector(
        result,
        rows,
        context,
        args[0]->encoding() == VectorEncoding::Simple::FLAT ? args[0]
                                                            : emptyVectorPtr);

    BufferPtr resultValues =
        (*result)->as<FlatVector<StringView>>()->mutableValues(rows.end());
    StringView* rawResult = resultValues->asMutable<StringView>();

    // If all of the vectors are flat or constant directly access the data in
    // the inner loop (1) We are not optimizing a case in which all of the
    // inputs are constant! (2) We are not optimizing a case in which start is
    // constant and length is vector (3) We are not optimizing a case in which
    // start is vector and length is constant
    // TODO: If turns out case 1..3 are popular we need to optimize them as well
    if (stringArgVectorEncoding == VectorEncoding::Simple::FLAT) {
      const StringView* rawStrings =
          stringsVector->as<FlatVector<StringView>>()->rawValues();
      if (startArgVectorEncoding == VectorEncoding::Simple::FLAT) {
        if (lengthArgVectorEncoding == VectorEncoding::Simple::FLAT) {
          const I* rawStarts = startsVector->as<FlatVector<I>>()->rawValues();
          // Incrementing ref count of the string buffer of the input vector so
          // that if the argument owner goes out of scope the string buffer
          // still remains in memory.
          (*result)->as<FlatVector<StringView>>()->acquireSharedStringBuffers(
              stringsVector->as<FlatVector<StringView>>());
          const I* rawLengths = noLengthVector
              ? nullptr
              : lengthsVector->as<FlatVector<I>>()->rawValues();
          // Fast path 1 (all Flat): apply the inner kernel in a loop
          if (noLengthVector) {
            rows.applyToSelected([&](int row) {
              applyInner<I>(
                  rawStarts[row],
                  std::numeric_limits<I>::max(),
                  rawStrings[row],
                  rawResult[row],
                  ascii);
            });
          } else {
            rows.applyToSelected([&](int row) {
              applyInner<I>(
                  rawStarts[row],
                  rawLengths[row],
                  rawStrings[row],
                  rawResult[row],
                  ascii);
            });
          }
          return;
        }
      }
      if (startArgVectorEncoding == VectorEncoding::Simple::CONSTANT &&
          lengthArgVectorEncoding == VectorEncoding::Simple::CONSTANT) {
        // Incrementing ref count of the string buffer of the input vector so
        // that if the argument owner goes out of scope the string buffer
        // still remains in memory.
        (*result)->as<FlatVector<StringView>>()->acquireSharedStringBuffers(
            stringsVector->as<FlatVector<StringView>>());
        int startIndex = startsVector->as<ConstantVector<I>>()->valueAt(0);
        I length = noLengthVector
            ? std::numeric_limits<I>::max()
            : lengthsVector->as<ConstantVector<I>>()->valueAt(0);
        // Fast path 2 (Constant start and length):
        rows.applyToSelected([&](int row) {
          applyInner<I>(
              startIndex, length, rawStrings[row], rawResult[row], ascii);
        });
        return;
      }
    }

    // The rest of the cases are handled through this slow path
    // which involves decoding all inputs and no direct access
    exec::LocalDecodedVector stringHolder(context, *stringsVector, rows);
    exec::LocalDecodedVector startHolder(context, *startsVector, rows);
    exec::LocalDecodedVector lengthHolder(context);
    auto strings = stringHolder.get();
    auto starts = startHolder.get();
    DecodedVector* lengths = noLengthVector ? nullptr : lengthHolder.get();
    if (!noLengthVector) {
      lengths->decode(*lengthsVector, rows);
    }
    auto baseVector = strings->base();
    (*result)->as<FlatVector<StringView>>()->acquireSharedStringBuffers(
        baseVector);

    if (noLengthVector) {
      rows.applyToSelected([&](int row) {
        applyInner<I>(
            starts->valueAt<I>(row),
            std::numeric_limits<I>::max(),
            strings->valueAt<StringView>(row),
            rawResult[row],
            ascii);
      });
    } else {
      rows.applyToSelected([&](int row) {
        applyInner<I>(
            starts->valueAt<I>(row),
            lengths->valueAt<I>(row),
            strings->valueAt<StringView>(row),
            rawResult[row],
            ascii);
      });
    }
  }

  bool ensureStringEncodingSetAtAllInputs() const override {
    return true;
  }

  bool propagateStringEncodingFromAllInputs() const override {
    return true;
  }
};

/**
 * Upper and Lower functions have a fast path for ascii where the functions
 * can be applied in place.
 * */
template <bool isLower /*instantiate for upper or lower*/>
class UpperLowerTemplateFunction : public exec::VectorFunction {
 private:
  /// String encoding wrappable function
  template <bool isAscii>
  struct ApplyInternal {
    static void apply(
        const SelectivityVector& rows,
        const DecodedVector* decodedInput,
        FlatVector<StringView>* results) {
      rows.applyToSelected([&](int row) {
        auto proxy = exec::StringProxy<FlatVector<StringView>>(results, row);
        if constexpr (isLower) {
          stringImpl::lower<isAscii>(
              proxy, decodedInput->valueAt<StringView>(row));
        } else {
          stringImpl::upper<isAscii>(
              proxy, decodedInput->valueAt<StringView>(row));
        }
        proxy.finalize();
      });
    }
  };

  void applyInternalInPlace(
      const SelectivityVector& rows,
      DecodedVector* decodedInput,
      FlatVector<StringView>* results) const {
    rows.applyToSelected([&](int row) {
      auto proxy =
          exec::StringProxy<FlatVector<StringView>, true /*reuseInput*/>(
              results,
              row,
              decodedInput->valueAt<StringView>(row) /*reusedInput*/,
              true /*inPlace*/);
      if constexpr (isLower) {
        stringImpl::lowerAsciiInPlace(proxy);
      } else {
        stringImpl::upperAsciiInPlace(proxy);
      }
      proxy.finalize();
    });
  }

 public:
  bool isDefaultNullBehavior() const override {
    return true;
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      [[maybe_unused]] exec::Expr* caller,
      exec::EvalCtx* context,
      VectorPtr* result) const override {
    VELOX_CHECK(args.size() == 1);
    VELOX_CHECK(args[0]->typeKind() == TypeKind::VARCHAR);

    // Read content before calling prepare results
    BaseVector* inputStringsVector = args[0].get();
    exec::LocalDecodedVector inputHolder(context, *inputStringsVector, rows);
    auto decodedInput = inputHolder.get();

    auto ascii = isAscii(inputStringsVector, rows);

    bool inPlace = ascii &&
        (inputStringsVector->encoding() == VectorEncoding::Simple::FLAT) &&
        hasSingleReferencedBuffers(
                       args.at(0).get()->as<FlatVector<StringView>>());

    if (inPlace) {
      bool inputVectorMoved =
          prepareFlatResultsVector(result, rows, context, args.at(0));
      auto* resultFlatVector = (*result)->as<FlatVector<StringView>>();

      // Move string buffers references to the output vector if we are reusing
      // them and they are not already moved
      if (!inputVectorMoved) {
        resultFlatVector->acquireSharedStringBuffers(inputStringsVector);
      }

      applyInternalInPlace(rows, decodedInput, resultFlatVector);
      return;
    }

    // Not in place path
    prepareFlatResultsVector(result, rows, context);
    auto* resultFlatVector = (*result)->as<FlatVector<StringView>>();

    StringEncodingTemplateWrapper<ApplyInternal>::apply(
        ascii, rows, decodedInput, resultFlatVector);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // varchar -> varchar
    return {exec::FunctionSignatureBuilder()
                .returnType("varchar")
                .argumentType("varchar")
                .build()};
  }

  bool ensureStringEncodingSetAtAllInputs() const override {
    return true;
  }

  bool propagateStringEncodingFromAllInputs() const override {
    return true;
  }
};

/**
 * concat(string1, ..., stringN) → varchar
 * Returns the concatenation of string1, string2, ..., stringN. This function
 * provides the same functionality as the SQL-standard concatenation operator
 * (||).
 * */
class ConcatFunction : public exec::VectorFunction {
 public:
  bool isDefaultNullBehavior() const override {
    return true;
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      exec::Expr* /* unused */,
      exec::EvalCtx* context,
      VectorPtr* result) const override {
    prepareFlatResultsVector(result, rows, context);
    auto* resultFlatVector = (*result)->as<FlatVector<StringView>>();

    exec::DecodedArgs decodedArgs(rows, args, context);

    std::vector<StringView> concatInputs(args.size());

    rows.applyToSelected([&](int row) {
      for (int i = 0; i < args.size(); i++) {
        concatInputs[i] = decodedArgs.at(i)->valueAt<StringView>(row);
      }
      auto proxy =
          exec::StringProxy<FlatVector<StringView>>(resultFlatVector, row);
      stringImpl::concatDynamic(proxy, concatInputs);
      proxy.finalize();
    });
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // varchar -> varchar
    return {exec::FunctionSignatureBuilder()
                .returnType("varchar")
                .argumentType("varchar")
                .variableArity()
                .build()};
  }

  bool propagateStringEncodingFromAllInputs() const override {
    return true;
  }
};

/**
 * strpos(string, substring) → bigint
 * Returns the starting position of the first instance of substring in string.
 * Positions start with 1. If not found, 0 is returned.
 *
 * strpos(string, substring, instance) → bigint
 * Returns the position of the N-th instance of substring in string. instance
 * must be a positive number. Positions start with 1. If not found, 0 is
 * returned.
 **/
class StringPosition : public exec::VectorFunction {
 private:
  /// A function that can be wrapped with ascii mode
  template <bool isAscii>
  struct ApplyInternal {
    template <
        typename StringReader,
        typename SubStringReader,
        typename InstanceReader>
    static void apply(
        StringReader stringReader,
        SubStringReader subStringReader,
        InstanceReader instanceReader,
        const SelectivityVector& rows,
        FlatVector<int64_t>* resultFlatVector) {
      rows.applyToSelected([&](int row) {
        auto result = stringImpl::stringPosition<isAscii>(
            stringReader(row), subStringReader(row), instanceReader(row));
        resultFlatVector->set(row, result);
      });
    }
  };

 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      exec::Expr* /* unused */,
      exec::EvalCtx* context,
      VectorPtr* result) const override {
    // Read string input
    exec::LocalDecodedVector decodedStringHolder(context, *args[0], rows);
    auto decodedStringInput = decodedStringHolder.get();

    // Read substring input
    exec::LocalDecodedVector decodedSubStringHolder(context, *args[1], rows);
    auto decodedSubStringInput = decodedSubStringHolder.get();

    // Read instance input
    exec::LocalDecodedVector decodedInstanceHolder(context);
    auto decodedInstanceInput = decodedInstanceHolder.get();

    std::optional<int64_t> instanceArgValue;
    if (args.size() <= 2) {
      instanceArgValue = 1;
    } else {
      decodedInstanceInput->decode(*args.at(2), rows);
      if (decodedInstanceInput->isConstantMapping()) {
        instanceArgValue = decodedInstanceInput->valueAt<int64_t>(0);
      }
    }

    auto stringArgStringEncoding = isAscii(args.at(0).get(), rows);
    BaseVector::ensureWritable(rows, BIGINT(), context->pool(), result);

    auto* resultFlatVector = (*result)->as<FlatVector<int64_t>>();

    auto stringReader = [&](const vector_size_t row) {
      return decodedStringInput->valueAt<StringView>(row);
    };

    auto substringReader = [&](const vector_size_t row) {
      return decodedSubStringInput->valueAt<StringView>(row);
    };

    auto instanceReader = [&](const vector_size_t row) {
      if (instanceArgValue.has_value()) {
        return instanceArgValue.value();
      } else {
        return decodedInstanceInput->valueAt<int64_t>(row);
      }
    };

    StringEncodingTemplateWrapper<ApplyInternal>::apply(
        stringArgStringEncoding,
        stringReader,
        substringReader,
        instanceReader,
        rows,
        resultFlatVector);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    return {
        // varchar, varchar -> bigint
        exec::FunctionSignatureBuilder()
            .returnType("bigint")
            .argumentType("varchar")
            .argumentType("varchar")
            .build(),
        // varchar, varchar, integer -> bigint
        exec::FunctionSignatureBuilder()
            .returnType("bigint")
            .argumentType("varchar")
            .argumentType("varchar")
            .argumentType("integer")
            .build(),
        // varchar, varchar, bigint -> bigint
        exec::FunctionSignatureBuilder()
            .returnType("bigint")
            .argumentType("varchar")
            .argumentType("varchar")
            .argumentType("bigint")
            .build(),
    };
  }
};

/**
 * replace(string, search) → varchar
 * Removes all instances of search from string.
 *
 * replace(string, search, replace) → varchar
 * Replaces all instances of search with replace in string.
 * If search is an empty string, inserts replace in front of every character
 *and at the end of the string.
 **/
class Replace : public exec::VectorFunction {
 private:
  template <
      typename StringReader,
      typename SearchReader,
      typename ReplaceReader>
  void applyInternal(
      StringReader stringReader,
      SearchReader searchReader,
      ReplaceReader replaceReader,
      const SelectivityVector& rows,
      FlatVector<StringView>* results) const {
    rows.applyToSelected([&](int row) {
      auto proxy = exec::StringProxy<FlatVector<StringView>>(results, row);
      stringImpl::replace(
          proxy, stringReader(row), searchReader(row), replaceReader(row));
      proxy.finalize();
    });
  }

  template <
      typename StringReader,
      typename SearchReader,
      typename ReplaceReader>
  void applyInPlace(
      StringReader stringReader,
      SearchReader searchReader,
      ReplaceReader replaceReader,
      const SelectivityVector& rows,
      FlatVector<StringView>* results) const {
    rows.applyToSelected([&](int row) {
      auto proxy =
          exec::StringProxy<FlatVector<StringView>, true /*reuseInput*/>(
              results,
              row,
              stringReader(row) /*reusedInput*/,
              true /*inPlace*/);
      stringImpl::replaceInPlace(proxy, searchReader(row), replaceReader(row));
      proxy.finalize();
    });
  }

 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      exec::Expr* /* unused */,
      exec::EvalCtx* context,
      VectorPtr* result) const override {
    // Read string input
    exec::LocalDecodedVector decodedStringHolder(context, *args[0], rows);
    auto decodedStringInput = decodedStringHolder.get();

    // Read search argument
    exec::LocalDecodedVector decodedSearchHolder(context, *args[1], rows);
    auto decodedSearchInput = decodedSearchHolder.get();

    std::optional<StringView> searchArgValue;
    if (decodedSearchInput->isConstantMapping()) {
      searchArgValue = decodedSearchInput->valueAt<StringView>(0);
    }

    // Read replace argument
    exec::LocalDecodedVector decodedReplaceHolder(context);
    auto decodedReplaceInput = decodedReplaceHolder.get();
    std::optional<StringView> replaceArgValue;

    if (args.size() <= 2) {
      replaceArgValue = StringView("");
    } else {
      decodedReplaceInput->decode(*args.at(2), rows);
      if (decodedReplaceInput->isConstantMapping()) {
        replaceArgValue = decodedReplaceInput->valueAt<StringView>(0);
      }
    }

    auto stringReader = [&](const vector_size_t row) {
      return decodedStringInput->valueAt<StringView>(row);
    };

    auto searchReader = [&](const vector_size_t row) {
      return decodedSearchInput->valueAt<StringView>(row);
    };

    auto replaceReader = [&](const vector_size_t row) {
      if (replaceArgValue.has_value()) {
        return replaceArgValue.value();
      } else {
        return decodedReplaceInput->valueAt<StringView>(row);
      }
    };

    // Right now we enable the inplace if 'search' and 'replace' are constants
    // and 'search' size is smaller than or equal to 'replace'.

    // TODO: analyze other options for enabling inplace i.e.:
    // 1. Decide per row.
    // 2. Scan inputs for max lengths and decide based on that. ..etc
    bool inPlace = replaceArgValue.has_value() && searchArgValue.has_value() &&
        (searchArgValue.value().size() <= replaceArgValue.value().size()) &&
        (args.at(0)->encoding() == VectorEncoding::Simple::FLAT) &&
        hasSingleReferencedBuffers(
                       args.at(0).get()->as<FlatVector<StringView>>());

    if (inPlace) {
      bool inputVectorMoved =
          prepareFlatResultsVector(result, rows, context, args.at(0));
      auto* resultFlatVector = (*result)->as<FlatVector<StringView>>();

      // Move string buffers references to the output vector if we are reusing
      // them and they are not already moved.
      if (!inputVectorMoved) {
        resultFlatVector->acquireSharedStringBuffers(args.at(0).get());
      }
      applyInPlace(
          stringReader, searchReader, replaceReader, rows, resultFlatVector);
      return;
    }
    // Not in place path
    prepareFlatResultsVector(result, rows, context);
    auto* resultFlatVector = (*result)->as<FlatVector<StringView>>();

    applyInternal(
        stringReader, searchReader, replaceReader, rows, resultFlatVector);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    return {
        // varchar, varchar -> varchar
        exec::FunctionSignatureBuilder()
            .returnType("varchar")
            .argumentType("varchar")
            .argumentType("varchar")
            .build(),
        // varchar, varchar, varchar -> varchar
        exec::FunctionSignatureBuilder()
            .returnType("varchar")
            .argumentType("varchar")
            .argumentType("varchar")
            .argumentType("varchar")
            .build(),
    };
  }

  // Only the original string and the replacement are relevant to the result
  // encoding.
  // TODO: The propagation is a safe approximation here, it might be better
  // for some cases to keep it unset and then rescan.
  std::optional<std::vector<size_t>> propagateStringEncodingFrom()
      const override {
    return {{0, 2}};
  }
};
} // namespace

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_substr,
    SubstrFunction::signatures(),
    std::make_unique<SubstrFunction>());

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_upper,
    UpperLowerTemplateFunction<false /*isLower*/>::signatures(),
    std::make_unique<UpperLowerTemplateFunction<false /*isLower*/>>());

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_lower,
    UpperLowerTemplateFunction<true /*isLower*/>::signatures(),
    std::make_unique<UpperLowerTemplateFunction<true /*isLower*/>>());

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_concat,
    ConcatFunction::signatures(),
    std::make_unique<ConcatFunction>());

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_strpos,
    StringPosition::signatures(),
    std::make_unique<StringPosition>());

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_replace,
    Replace::signatures(),
    std::make_unique<Replace>());

} // namespace facebook::velox::functions
