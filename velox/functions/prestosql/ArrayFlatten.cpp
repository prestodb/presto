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

#include "velox/functions/prestosql/ArrayFunctions.h"

namespace facebook::velox::functions {
namespace {

class ArrayFlattenFunction : public exec::VectorFunction {
 public:
  static std::vector<exec::FunctionSignaturePtr> signatures() {
    // array(array(T)) -> array(T)
    return {exec::FunctionSignatureBuilder()
                .returnType("array(T)")
                .argumentType("array(array(T))")
                .typeVariable("T")
                .build()};
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    VELOX_CHECK_EQ(args.size(), 1);
    const auto& arrayVector = args[0];
    VELOX_CHECK_EQ(arrayVector->encoding(), VectorEncoding::Simple::ARRAY);
    const auto* arrayVectorPtr = arrayVector->as<ArrayVector>();
    auto state =
        ProcessState::create(outputType, rows.end(), arrayVectorPtr, context);
    rows.applyToSelected(
        [&](vector_size_t row) { processOneInputArray(row, row, state); });
    VectorPtr localResult;
    if (state.flattenElementsConsecutive) {
      localResult = flattenArray(state);
    } else {
      localResult = flattenArrayWithDictionary(rows, state);
    }
    context.moveOrCopyResult(localResult, rows, result);
  }

 private:
  struct ProcessState {
    TypePtr resultType;
    vector_size_t numRows{0};

    const vector_size_t* arrayOffsets{};
    const vector_size_t* arraySizes{};

    std::unique_ptr<exec::LocalSelectivityVector> innerArrayRowSelector;
    std::unique_ptr<exec::LocalDecodedVector> decodedInnerArrayHolder;
    const DecodedVector* decodedInnerArray{nullptr};
    VectorPtr elements{nullptr};
    const vector_size_t* elementOffsets{nullptr};
    const vector_size_t* elementSizes{nullptr};

    BufferPtr flattenArrayOffsets{nullptr};
    vector_size_t* rawFlattenArrayOffsets{nullptr};
    BufferPtr flattenArraySizes{nullptr};
    vector_size_t* rawFlattenArraySizes{nullptr};

    // Indicates if the flatten elements are consecutive in source base element
    // vector. If so, we can directly reference the source element vector.
    // Otherwise, we need to build a dictionary mapping on top of the source
    // element vector.
    bool flattenElementsConsecutive{true};
    vector_size_t numFlattenElements{0};
    BufferPtr flattenElementIndices{nullptr};
    vector_size_t* rawFlattenElementIndices{nullptr};
    VectorPtr flattenElementsWithDictionary{nullptr};

    memory::MemoryPool* pool{nullptr};

    std::pair<vector_size_t, vector_size_t> elementOffsetAndSize(
        vector_size_t elementIndex) const {
      return {elementOffsets[elementIndex], elementSizes[elementIndex]};
    }

    // @param arrayVector ARRAY(ARRAY(E)) vector.
    static ProcessState create(
        const TypePtr& resultType,
        size_t numRows,
        const ArrayVector* arrayVector,
        exec::EvalCtx& context) {
      VELOX_CHECK_NOT_NULL(arrayVector);
      ProcessState state;
      state.resultType = resultType;
      state.numRows = numRows;
      state.pool = context.pool();

      state.arrayOffsets = arrayVector->rawOffsets();
      state.arraySizes = arrayVector->rawSizes();

      const auto innerArrayVector = arrayVector->elements();
      state.innerArrayRowSelector =
          std::make_unique<exec::LocalSelectivityVector>(
              context, innerArrayVector->size());
      state.innerArrayRowSelector->get()->setAll();
      state.decodedInnerArrayHolder =
          std::make_unique<exec::LocalDecodedVector>(
              context, *innerArrayVector, *state.innerArrayRowSelector->get());
      state.decodedInnerArray = state.decodedInnerArrayHolder->get();

      auto* innerArrayVectorPtr =
          state.decodedInnerArray->base()->as<ArrayVector>();
      state.elements = innerArrayVectorPtr->elements();
      state.elementOffsets = innerArrayVectorPtr->rawOffsets();
      state.elementSizes = innerArrayVectorPtr->rawSizes();

      state.flattenArrayOffsets = allocateIndices(state.numRows, state.pool);
      state.rawFlattenArrayOffsets =
          state.flattenArrayOffsets->asMutable<velox::vector_size_t>();

      state.flattenArraySizes = allocateIndices(state.numRows, state.pool);
      state.rawFlattenArraySizes =
          state.flattenArraySizes->asMutable<velox::vector_size_t>();
      return state;
    }
  };

  VectorPtr flattenArrayWithDictionary(
      const SelectivityVector& rows,
      ProcessState& state) const {
    createFlattenElementIndices(state);
    vector_size_t flattenElementOffset{0};
    rows.applyToSelected([&](vector_size_t row) {
      processOneOutputRow(row, row, state, flattenElementOffset);
    });
    return std::make_shared<velox::ArrayVector>(
        state.pool,
        state.resultType,
        nullptr,
        state.numRows,
        state.flattenArrayOffsets,
        state.flattenArraySizes,
        state.flattenElementsWithDictionary);
  }

  static void createFlattenElementIndices(ProcessState& state) {
    VELOX_CHECK(!state.flattenElementsConsecutive);
    VELOX_CHECK_NULL(state.flattenElementIndices);
    VELOX_CHECK_NULL(state.flattenElementsWithDictionary);
    state.flattenElementIndices =
        allocateIndices(state.numFlattenElements, state.pool);
    state.rawFlattenElementIndices =
        state.flattenElementIndices->asMutable<vector_size_t>();
    state.flattenElementsWithDictionary = BaseVector::wrapInDictionary(
        nullptr,
        state.flattenElementIndices,
        state.numFlattenElements,
        state.elements);
  }

  VectorPtr flattenArray(const ProcessState& state) const {
    return std::make_shared<velox::ArrayVector>(
        state.pool,
        state.resultType,
        nullptr,
        state.numRows,
        state.flattenArrayOffsets,
        state.flattenArraySizes,
        state.elements);
  }

  // @param inputRow The row index in the input ARRAY(ARRAY(E)) vector.
  // @param outputRow The row index in the output ARRAY(E) vector.
  void processOneInputArray(
      vector_size_t inputRow,
      vector_size_t outputRow,
      ProcessState& state) const {
    vector_size_t nextElementOffset{-1};
    for (auto i = 0, offset = state.arrayOffsets[inputRow];
         i < state.arraySizes[inputRow];
         ++i, ++offset) {
      if (FOLLY_UNLIKELY(state.decodedInnerArray->isNullAt(offset))) {
        continue;
      }
      const auto elementArrayIndex = state.decodedInnerArray->index(offset);
      const auto [elementOffset, numElements] =
          state.elementOffsetAndSize(elementArrayIndex);
      state.numFlattenElements += numElements;
      state.rawFlattenArraySizes[outputRow] += numElements;
      if (nextElementOffset == -1) {
        state.rawFlattenArrayOffsets[outputRow] = elementOffset;
      } else if (nextElementOffset != elementOffset) {
        state.flattenElementsConsecutive = false;
      }
      nextElementOffset = elementOffset + numElements;
    }
  }

  void processOneOutputRow(
      vector_size_t inputRow,
      vector_size_t outputRow,
      const ProcessState& state,
      vector_size_t& flattenElementOffset) const {
    state.rawFlattenArrayOffsets[outputRow] = flattenElementOffset;
    vector_size_t numFlattenElements{0};
    for (auto i = 0, elementArrayOffset = state.arrayOffsets[inputRow];
         i < state.arraySizes[inputRow];
         ++i, ++elementArrayOffset) {
      if (FOLLY_UNLIKELY(
              state.decodedInnerArray->isNullAt(elementArrayOffset))) {
        continue;
      }
      const auto elementArrayIndex =
          state.decodedInnerArray->index(elementArrayOffset);
      const auto [elementOffset, numElements] =
          state.elementOffsetAndSize(elementArrayIndex);
      for (auto elementIndex = elementOffset;
           elementIndex < elementOffset + numElements;
           ++elementIndex) {
        state.rawFlattenElementIndices[flattenElementOffset++] = elementIndex;
      }
      numFlattenElements += numElements;
    }
    VELOX_CHECK_EQ(numFlattenElements, state.rawFlattenArraySizes[outputRow]);
  }
};

} // namespace

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_array_flatten,
    ArrayFlattenFunction::signatures(),
    std::make_unique<ArrayFlattenFunction>());

} // namespace facebook::velox::functions
