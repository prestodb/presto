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

#include "velox/expression/Expr.h"
#include "velox/expression/VectorFunction.h"
#include "velox/type/Type.h"

namespace facebook::velox::functions {
namespace {

// See documentation at
//   - https://prestodb.io/docs/current/functions/array.html
/// This class is used for slice function with presto semantics).
///
/// For a query slice(input, 2, 2):
/// Input ArrayVector is
/// [
///  [1, 2, 3]
///  [4, 5, 6, 7]
///  [8, 9, 10, 11, 12]
/// ]
/// Output ArrayVector is (with default presto behavior) is
/// [
///  [2, 3]
///  [5, 6]
///  [9, 10]
/// ]
///
/// The function achieves zero copy through re-using base vector and adjusting
/// the rawOffsets and rawSizes vectors.
/// For the input ArrayVector:
/// rawOffsets vector [0, 3, 7]
/// rawSizes vector   [3, 4, 5]
///
/// After adjustment, for the output ArrayVector:
/// rawOffsets vector [1, 4, 8]
/// rawSizes vector   [2, 2, 2]
class SliceFunction : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    VELOX_USER_CHECK_EQ(
        args[0]->typeKind(),
        TypeKind::ARRAY,
        "Function slice() requires first argument of type ARRAY");
    VELOX_USER_CHECK_EQ(
        args[1]->typeKind(),
        TypeKind::BIGINT,
        "Function slice() requires second argument of type BIGINT");
    VELOX_USER_CHECK_EQ(
        args[1]->typeKind(),
        args[2]->typeKind(),
        "Function slice() requires start and length to be the same type");

    VectorPtr localResult =
        applyArray<int64_t>(rows, args, context, outputType);
    context.moveOrCopyResult(localResult, rows, result);
  }

 private:
  // Use template parameter rather than hard-coded TypeKind to specify array
  // data type.
  template <typename T>
  VectorPtr applyArray(
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      exec::EvalCtx& context,
      const TypePtr& outputType) const {
    auto pool = context.pool();
    BufferPtr offsets = allocateOffsets(rows.end(), pool);
    auto rawOffsets = offsets->asMutable<vector_size_t>();
    BufferPtr sizes = allocateSizes(rows.end(), pool);
    auto rawSizes = sizes->asMutable<vector_size_t>();

    exec::DecodedArgs decodedArgs(rows, args, context);
    auto decodedArray = decodedArgs.at(0);
    auto baseArray = decodedArray->base()->as<ArrayVector>();
    auto arrayIndices = decodedArray->indices();
    auto baseRawSizes = baseArray->rawSizes();
    auto baseRawOffsets = baseArray->rawOffsets();

    auto decodedStart = decodedArgs.at(1);
    auto decodedLength = decodedArgs.at(2);

    const auto fillResultVectorFunc = [&](vector_size_t row,
                                          vector_size_t adjustedStart) {
      auto arraySize = baseRawSizes[arrayIndices[row]];
      auto index = getIndex(adjustedStart, arraySize);
      if (index != -1) {
        auto start = baseRawOffsets[arrayIndices[row]] + index;
        rawOffsets[row] = start;
        rawSizes[row] = adjustLength(
            start,
            // Indices are always 32-bit integers, template arguments are used
            // to accommodate more data types.
            static_cast<vector_size_t>(decodedLength->valueAt<T>(row)),
            row,
            baseRawSizes,
            baseRawOffsets,
            arrayIndices);
      }
    };

    if (decodedStart->isConstantMapping()) {
      // The save here is that if the constant is invalid, no need to perform
      // computation over and over again.
      try {
        vector_size_t adjustedStart = adjustIndex(
            static_cast<vector_size_t>(decodedStart->valueAt<T>(0)));
        context.applyToSelectedNoThrow(
            rows, [&](auto row) { fillResultVectorFunc(row, adjustedStart); });
      } catch (const std::exception& /*e*/) {
        context.setErrors(rows, std::current_exception());
      }
    } else {
      context.applyToSelectedNoThrow(rows, [&](auto row) {
        auto adjustedStart = adjustIndex(
            static_cast<vector_size_t>(decodedStart->valueAt<T>(row)));
        fillResultVectorFunc(row, adjustedStart);
      });
    }
    return std::make_shared<ArrayVector>(
        pool,
        outputType,
        nullptr,
        rows.end(),
        offsets,
        sizes,
        baseArray->elements());
  }

  // Presto has the semantics of starting index from one, need to add a minus
  // one offset here.
  vector_size_t adjustIndex(vector_size_t index) const {
    // If it's zero, throw.
    if (UNLIKELY(index == 0)) {
      VELOX_USER_FAIL("SQL array indices start at 1");
    }

    // If larger than zero, adjust it.
    if (index > 0) {
      index--;
    }
    return index;
  }

  vector_size_t getIndex(vector_size_t start, vector_size_t size) const {
    if (start < 0) {
      // If we see negative start, we wrap it around by size amount.
      start += size;
    }

    // Check if start is within bound.
    if ((start >= size) || (start < 0)) {
      // Return -1 when start is out of bound, caller will make it an empty
      // array.
      return -1;
    }

    return start;
  }

  vector_size_t adjustLength(
      vector_size_t start,
      vector_size_t length,
      vector_size_t row,
      const vector_size_t* rawSizes,
      const vector_size_t* rawOffsets,
      const vector_size_t* indices) const {
    if (length < 0) {
      VELOX_USER_FAIL(
          "The value of length argument of slice() function should not be negative");
    }
    auto endIndex = rawOffsets[indices[row]] + rawSizes[indices[row]];
    return std::min(endIndex - start, length);
  }
};

static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
  return {// array(T, bigint, bigint) -> array(T)
          exec::FunctionSignatureBuilder()
              .typeVariable("T")
              .returnType("array(T)")
              .argumentType("array(T)")
              .argumentType("bigint")
              .argumentType("bigint")
              .build()};
}

} // namespace

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_slice,
    signatures(),
    std::make_unique<SliceFunction>());
} // namespace facebook::velox::functions
