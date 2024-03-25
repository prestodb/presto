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
#include "velox/expression/DecodedArgs.h"
#include "velox/expression/VectorFunction.h"

namespace facebook::velox::functions {
namespace {
// See documentation at https://prestodb.io/docs/current/functions/array.html
class RepeatFunction : public exec::VectorFunction {
 public:
  // @param allowNegativeCount If true, negative 'count' is allowed
  // and treated the same as zero (Spark's behavior).
  explicit RepeatFunction(bool allowNegativeCount)
      : allowNegativeCount_(allowNegativeCount) {}

  static constexpr int32_t kMaxResultEntries = 10'000;

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    VectorPtr localResult;
    if (args[1]->isConstantEncoding()) {
      localResult = applyConstantCount(rows, args, outputType, context);
      if (localResult == nullptr) {
        return;
      }
    } else {
      localResult = applyNonConstantCount(rows, args, outputType, context);
    }
    context.moveOrCopyResult(localResult, rows, result);
  }

 private:
  // Check count to make sure it is in valid range.
  static int32_t checkCount(int32_t count, bool allowNegativeCount) {
    if (count < 0) {
      if (allowNegativeCount) {
        return 0;
      }
      VELOX_USER_FAIL(
          "({} vs. {}) Count argument of repeat function must be greater than or equal to 0",
          count,
          0);
    }
    VELOX_USER_CHECK_LE(
        count,
        kMaxResultEntries,
        "Count argument of repeat function must be less than or equal to 10000");
    return count;
  }

  VectorPtr applyConstantCount(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context) const {
    const auto numRows = rows.end();
    auto pool = context.pool();

    auto* constantCount = args[1]->as<ConstantVector<int32_t>>();
    if (constantCount->isNullAt(0)) {
      // If count is a null constant, the result should be all nulls.
      return BaseVector::createNullConstant(outputType, numRows, pool);
    }

    auto count = constantCount->valueAt(0);
    try {
      count = checkCount(count, allowNegativeCount_);
    } catch (const VeloxUserError&) {
      context.setErrors(rows, std::current_exception());
      return nullptr;
    }
    const auto totalCount = count * numRows;

    // Allocate new vectors for indices, lengths and offsets.
    BufferPtr indices = allocateIndices(totalCount, pool);
    BufferPtr sizes = allocateSizes(numRows, pool);
    BufferPtr offsets = allocateOffsets(numRows, pool);
    auto rawIndices = indices->asMutable<vector_size_t>();
    auto rawSizes = sizes->asMutable<vector_size_t>();
    auto rawOffsets = offsets->asMutable<vector_size_t>();

    vector_size_t offset = 0;
    context.applyToSelectedNoThrow(rows, [&](auto row) {
      rawSizes[row] = count;
      rawOffsets[row] = offset;
      std::fill(rawIndices + offset, rawIndices + offset + count, row);
      offset += count;
    });

    return std::make_shared<ArrayVector>(
        pool,
        outputType,
        nullptr,
        numRows,
        offsets,
        sizes,
        BaseVector::wrapInDictionary(nullptr, indices, totalCount, args[0]));
  }

  VectorPtr applyNonConstantCount(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context) const {
    exec::DecodedArgs decodedArgs(rows, args, context);
    auto countDecoded = decodedArgs.at(1);
    int32_t totalCount = 0;
    context.applyToSelectedNoThrow(rows, [&](auto row) {
      auto count =
          countDecoded->isNullAt(row) ? 0 : countDecoded->valueAt<int32_t>(row);
      count = checkCount(count, allowNegativeCount_);
      totalCount += count;
    });

    const auto numRows = rows.end();
    auto pool = context.pool();

    // Allocate new vector for nulls if necessary.
    BufferPtr nulls;
    uint64_t* rawNulls = nullptr;
    if (countDecoded->mayHaveNulls()) {
      nulls = allocateNulls(numRows, pool);
      rawNulls = nulls->asMutable<uint64_t>();
    }

    // Allocate new vectors for indices, lengths and offsets.
    BufferPtr indices = allocateIndices(totalCount, pool);
    BufferPtr sizes = allocateSizes(numRows, pool);
    BufferPtr offsets = allocateOffsets(numRows, pool);
    auto rawIndices = indices->asMutable<vector_size_t>();
    auto rawSizes = sizes->asMutable<vector_size_t>();
    auto rawOffsets = offsets->asMutable<vector_size_t>();

    // When context.throwOnError is false, rows with invalid count argument
    // should be deselected and not be processed further.
    SelectivityVector remainingRows = rows;
    context.deselectErrors(remainingRows);
    vector_size_t offset = 0;
    context.applyToSelectedNoThrow(remainingRows, [&](auto row) {
      if (rawNulls && countDecoded->isNullAt(row)) {
        // Returns null if count argument is null.
        // e.g. repeat('a', null) -> null
        bits::setNull(rawNulls, row);
        return;
      }
      auto count = countDecoded->valueAt<int32_t>(row);
      if (count < 0) {
        count = 0;
      }
      rawSizes[row] = count;
      rawOffsets[row] = offset;
      std::fill(rawIndices + offset, rawIndices + offset + count, row);
      offset += count;
    });

    return std::make_shared<ArrayVector>(
        pool,
        outputType,
        nulls,
        numRows,
        offsets,
        sizes,
        BaseVector::wrapInDictionary(nullptr, indices, totalCount, args[0]));
  }

  const bool allowNegativeCount_;
};
} // namespace

std::vector<std::shared_ptr<exec::FunctionSignature>> repeatSignatures() {
  // T, integer -> array(T)
  return {exec::FunctionSignatureBuilder()
              .typeVariable("T")
              .returnType("array(T)")
              .argumentType("T")
              .argumentType("integer")
              .build()};
}

exec::VectorFunctionMetadata repeatMetadata() {
  // repeat(null, n) returns an array of n nulls.
  return exec::VectorFunctionMetadataBuilder()
      .defaultNullBehavior(false)
      .build();
}

std::shared_ptr<exec::VectorFunction> makeRepeat(
    const std::string& /* name */,
    const std::vector<exec::VectorFunctionArg>& /* inputArgs */,
    const core::QueryConfig& /*config*/) {
  return std::make_unique<RepeatFunction>(false);
}

std::shared_ptr<exec::VectorFunction> makeRepeatAllowNegativeCount(
    const std::string& /* name */,
    const std::vector<exec::VectorFunctionArg>& /* inputArgs */,
    const core::QueryConfig& /*config*/) {
  return std::make_unique<RepeatFunction>(true);
}

} // namespace facebook::velox::functions
