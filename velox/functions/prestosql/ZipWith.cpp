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
#include "velox/expression/VarSetter.h"
#include "velox/expression/VectorFunction.h"
#include "velox/functions/lib/LambdaFunctionUtil.h"
#include "velox/vector/FunctionVector.h"

namespace facebook::velox::functions {
namespace {

struct Buffers {
  BufferPtr offsets;
  BufferPtr sizes;
  BufferPtr nulls;
  vector_size_t numElements;
};

struct DecodedInputs {
  DecodedVector* decodedLeft;
  DecodedVector* decodedRight;
  const ArrayVector* baseLeft;
  const ArrayVector* baseRight;

  DecodedInputs(DecodedVector* _decodedLeft, DecodedVector* _decodeRight)
      : decodedLeft{_decodedLeft},
        decodedRight{_decodeRight},
        baseLeft{decodedLeft->base()->asUnchecked<ArrayVector>()},
        baseRight{decodedRight->base()->asUnchecked<ArrayVector>()} {}
};

// See documentation at
// https://prestodb.io/docs/current/functions/array.html#zip_with
class ZipWithFunction : public exec::VectorFunction {
 public:
  bool isDefaultNullBehavior() const override {
    // zip_with is null preserving for the arrays, but since an
    // expr tree with a lambda depends on all named fields, including
    // captures, a null in a capture does not automatically make a
    // null result.
    return false;
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    VELOX_CHECK_EQ(args.size(), 3);
    exec::DecodedArgs decodedArgs(rows, {args[0], args[1]}, context);
    DecodedInputs decodedInputs{decodedArgs.at(0), decodedArgs.at(1)};

    // Number of elements in the result vector.
    // Sizes, offsets and nulls for the result ArrayVector.
    // Size of the result array is the max of sizes of the input arrays.
    // Result array is null if one or both of the input arrays are null.
    bool leftNeedsPadding = false;
    bool rightNeedsPadding = false;
    auto resultBuffers = computeResultBuffers(
        decodedInputs,
        rows,
        context.pool(),
        leftNeedsPadding,
        rightNeedsPadding);

    // If one array is shorter than the other, add nulls at the end of the
    // shorter array. Use dictionary encoding to represent elements of the
    // padded arrays.
    auto lambdaArgs = flattenAndPadArrays(
        decodedInputs,
        resultBuffers,
        rows,
        context.pool(),
        leftNeedsPadding,
        rightNeedsPadding);

    const auto numResultElements = resultBuffers.numElements;
    auto* rawOffsets = resultBuffers.offsets->as<vector_size_t>();
    auto* rawSizes = resultBuffers.sizes->as<vector_size_t>();

    const SelectivityVector allElementRows(numResultElements);

    VectorPtr newElements;

    // Loop over lambda functions and apply these to (leftElements,
    // rightElements). In most cases there will be only one function and the
    // loop will run once.
    auto it = args[2]->asUnchecked<FunctionVector>()->iterator(&rows);
    while (auto entry = it.next()) {
      // Optimize for the case of a single lambda expression and 'rows' covering
      // all rows.
      const bool allSelected = entry.rows->isAllSelected();
      SelectivityVector elementRows(numResultElements, allSelected);
      if (!allSelected) {
        entry.rows->applyToSelected([&](auto row) {
          elementRows.setValidRange(
              rawOffsets[row], rawOffsets[row] + rawSizes[row], true);
        });
        elementRows.updateBounds();
      }

      BufferPtr wrapCapture;
      if (entry.callable->hasCapture()) {
        wrapCapture = allocateIndices(numResultElements, context.pool());
        auto rawWrapCaptures = wrapCapture->asMutable<vector_size_t>();

        vector_size_t offset = 0;
        entry.rows->applyToSelected([&](auto row) {
          for (auto i = 0; i < rawSizes[row]; ++i) {
            rawWrapCaptures[offset++] = row;
          }
        });
      }

      // Make sure already populated entries in newElements do not get
      // overwritten.
      VarSetter finalSelection(
          context.mutableFinalSelection(), &allElementRows);
      VarSetter isFinalSelection(context.mutableIsFinalSelection(), false);

      entry.callable->apply(
          elementRows,
          allElementRows,
          wrapCapture,
          &context,
          lambdaArgs,
          &newElements);
    }

    auto localResult = std::make_shared<ArrayVector>(
        context.pool(),
        outputType,
        resultBuffers.nulls,
        rows.end(),
        resultBuffers.offsets,
        resultBuffers.sizes,
        newElements);
    context.moveOrCopyResult(localResult, rows, result);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // array(T), array(U), function(T, U, R) -> array(R)
    return {exec::FunctionSignatureBuilder()
                .typeVariable("T")
                .typeVariable("U")
                .typeVariable("R")
                .returnType("array(R)")
                .argumentType("array(T)")
                .argumentType("array(U)")
                .argumentType("function(T, U, R)")
                .build()};
  }

 private:
  static Buffers computeResultBuffers(
      const DecodedInputs& decodedInputs,
      const SelectivityVector& rows,
      memory::MemoryPool* pool,
      bool& leftNeedsPadding,
      bool& rightNeedsPadding) {
    BufferPtr sizes = allocateSizes(rows.end(), pool);
    auto* rawSizes = sizes->asMutable<vector_size_t>();

    BufferPtr offsets = allocateOffsets(rows.end(), pool);
    auto* rawOffsets = offsets->asMutable<vector_size_t>();

    BufferPtr nulls;
    uint64_t* rawNulls = nullptr;
    if (decodedInputs.decodedLeft->mayHaveNulls() ||
        decodedInputs.decodedRight->mayHaveNulls()) {
      nulls = allocateNulls(rows.end(), pool);
      rawNulls = nulls->asMutable<uint64_t>();
    }

    auto leftSizes = decodedInputs.baseLeft->rawSizes();
    auto rightSizes = decodedInputs.baseRight->rawSizes();

    vector_size_t offset = 0;
    rows.applyToSelected([&](auto row) {
      if (rawNulls &&
          (decodedInputs.decodedLeft->isNullAt(row) ||
           decodedInputs.decodedRight->isNullAt(row))) {
        bits::setNull(rawNulls, row);
        return;
      }

      auto leftRow = decodedInputs.decodedLeft->index(row);
      auto rightRow = decodedInputs.decodedRight->index(row);
      auto leftSize = leftSizes[leftRow];
      auto rightSize = rightSizes[rightRow];
      auto size = std::max(leftSize, rightSize);
      if (leftSize < size) {
        leftNeedsPadding = true;
      }
      if (rightSize < size) {
        rightNeedsPadding = true;
      }
      rawOffsets[row] = offset;
      rawSizes[row] = size;
      offset += size;
    });

    return {offsets, sizes, nulls, offset};
  }

  static bool areSameOffsets(
      const vector_size_t* offsets,
      const vector_size_t* desiredOffsets,
      vector_size_t size) {
    for (auto i = 0; i < size; ++i) {
      if (offsets[i] != desiredOffsets[i]) {
        return false;
      }
    }
    return true;
  }

  // Takes a decoded array vector and list of new array sizes. Returns a new
  // elements vector that have nulls added at the end of the arrays up to the
  // new sizes. The new elements vector has elements placed sequentially, e.g.
  // offset[N + 1] = offset[N] + size[N].
  // @param needsPaddiing is true if at least one array needs to grow to a
  // larger size.
  static VectorPtr flattenAndPadArray(
      DecodedVector* decoded,
      const ArrayVector* base,
      const SelectivityVector& rows,
      memory::MemoryPool* pool,
      vector_size_t numResultElements,
      const vector_size_t* resultOffsets,
      const vector_size_t* resultSizes,
      bool needsPadding) {
    auto* offsets = base->rawOffsets();
    auto* sizes = base->rawSizes();

    if (!needsPadding && decoded->isIdentityMapping() && rows.isAllSelected() &&
        areSameOffsets(offsets, resultOffsets, rows.size())) {
      return base->elements();
    }

    BufferPtr indices = allocateIndices(numResultElements, pool);
    auto* rawIndices = indices->asMutable<vector_size_t>();

    BufferPtr nulls;
    uint64_t* rawNulls = nullptr;
    if (needsPadding) {
      nulls = allocateNulls(numResultElements, pool);
      rawNulls = nulls->asMutable<uint64_t>();
    }

    vector_size_t resultOffset = 0;

    rows.applyToSelected([&](auto row) {
      const auto resultSize = resultSizes[row];
      if (resultSize == 0) {
        return;
      }

      auto baseRow = decoded->index(row);
      auto size = sizes[baseRow];
      auto offset = offsets[baseRow];

      for (auto i = 0; i < size; ++i) {
        rawIndices[resultOffset + i] = offset + i;
      }
      for (auto i = size; i < resultSize; ++i) {
        bits::setNull(rawNulls, resultOffset + i);
      }
      resultOffset += resultSize;
    });

    return BaseVector::wrapInDictionary(
        nulls, indices, numResultElements, base->elements());
  }

  static std::vector<VectorPtr> flattenAndPadArrays(
      const DecodedInputs& decodedInputs,
      const Buffers& resultBuffers,
      const SelectivityVector& rows,
      memory::MemoryPool* pool,
      bool leftNeedsPadding,
      bool rightNeedsPadding) {
    auto* resultSizes = resultBuffers.sizes->as<vector_size_t>();
    auto* resultOffsets = resultBuffers.offsets->as<vector_size_t>();

    auto paddedLeft = flattenAndPadArray(
        decodedInputs.decodedLeft,
        decodedInputs.baseLeft,
        rows,
        pool,
        resultBuffers.numElements,
        resultOffsets,
        resultSizes,
        leftNeedsPadding);

    auto paddedRight = flattenAndPadArray(
        decodedInputs.decodedRight,
        decodedInputs.baseRight,
        rows,
        pool,
        resultBuffers.numElements,
        resultOffsets,
        resultSizes,
        rightNeedsPadding);

    return {paddedLeft, paddedRight};
  }
};
} // namespace

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_zip_with,
    ZipWithFunction::signatures(),
    std::make_unique<ZipWithFunction>());

} // namespace facebook::velox::functions
