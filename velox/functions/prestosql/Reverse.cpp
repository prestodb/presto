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
#include "velox/expression/StringWriter.h"
#include "velox/expression/VectorFunction.h"
#include "velox/functions/lib/StringEncodingUtils.h"
#include "velox/functions/lib/string/StringImpl.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::functions {

///  reverse(Array[E]) -> Array[E]
///  Takes any array as an input and returns the reversed array.
///
///  reverse(Varchar) -> Varchar
///  Takes any Varchar as an input and returns the reversed varchar.
class ReverseFunction : public exec::VectorFunction {
 private:
  /// String encoding wrappable function
  template <bool isAscii>
  struct ApplyVarcharInternal {
    static void apply(
        const SelectivityVector& rows,
        const FlatVector<StringView>* input,
        FlatVector<StringView>* results) {
      rows.applyToSelected([&](int row) {
        auto proxy = exec::StringWriter<>(results, row);
        stringImpl::reverse<isAscii>(proxy, input->valueAt(row).getString());
        proxy.finalize();
      });
    }
  };

 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx* context,
      VectorPtr* result) const override {
    VELOX_CHECK_EQ(args.size(), 1);

    switch (args[0]->typeKind()) {
      case TypeKind::ARRAY:
        applyArray(rows, args, context, result);
        return;
      case TypeKind::VARCHAR:
        applyVarchar(rows, args, context, result);
        return;
      default:
        VELOX_FAIL(
            "Unsupported input type for 'reverse' function: {}",
            args[0]->type()->toString());
    }
  }

  void applyVarchar(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      exec::EvalCtx* context,
      VectorPtr* result) const {
    auto* arg = args[0].get();

    auto ascii = isAscii(arg, rows);

    prepareFlatResultsVector(result, rows, context, args[0]);
    auto* flatResult = (*result)->as<FlatVector<StringView>>();

    // Input can be constant or flat.
    if (arg->isConstantEncoding()) {
      auto value = arg->as<ConstantVector<StringView>>()->valueAt(0);

      auto proxy = exec::StringWriter<>(flatResult, rows.begin());
      if (ascii) {
        stringImpl::reverse<true>(proxy, value.str());
      } else {
        stringImpl::reverse<false>(proxy, value.str());
      }
      proxy.finalize();

      auto rawResults = flatResult->mutableRawValues();
      auto reversedValue = rawResults[rows.begin()];

      rows.applyToSelected([&](auto row) { rawResults[row] = reversedValue; });
    } else {
      auto flatInput = arg->as<FlatVector<StringView>>();

      StringEncodingTemplateWrapper<ApplyVarcharInternal>::apply(
          ascii, rows, flatInput, flatResult);
    }
  }

  bool ensureStringEncodingSetAtAllInputs() const override {
    return true;
  }

  bool propagateStringEncodingFromAllInputs() const override {
    return true;
  }

  void applyArray(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      exec::EvalCtx* context,
      VectorPtr* result) const {
    auto& arg = args[0];

    VectorPtr localResult;

    // Input can be constant or flat.
    if (arg->isConstantEncoding()) {
      auto* constantArray = arg->as<ConstantVector<ComplexType>>();
      const auto& flatArray = constantArray->valueVector();
      const auto flatIndex = constantArray->index();

      SelectivityVector singleRow(flatIndex + 1, false);
      singleRow.setValid(flatIndex, true);
      singleRow.updateBounds();

      localResult = applyArrayFlat(singleRow, flatArray, context);
      localResult =
          BaseVector::wrapInConstant(rows.size(), flatIndex, localResult);
    } else {
      localResult = applyArrayFlat(rows, arg, context);
    }

    context->moveOrCopyResult(localResult, rows, result);
  }

  VectorPtr applyArrayFlat(
      const SelectivityVector& rows,
      const VectorPtr& vector,
      exec::EvalCtx* context) const {
    auto arrayVector = vector->as<ArrayVector>();
    auto elementCount = arrayVector->elements()->size();

    // Allocate new vectors for indices.
    auto pool = context->pool();
    BufferPtr indices = allocateIndices(elementCount, pool);
    auto rawIndices = indices->asMutable<vector_size_t>();

    auto elementsVector = arrayVector->elements();
    auto rawSizes = arrayVector->rawSizes();
    auto rawOffsets = arrayVector->rawOffsets();

    rows.applyToSelected([&](auto row) {
      auto size = rawSizes[row];
      auto offset = rawOffsets[row];

      for (auto i = 0; i < size; ++i) {
        rawIndices[offset + i] = offset + size - i - 1;
      }
    });

    auto elementsDict =
        BaseVector::transpose(indices, std::move(elementsVector));

    return std::make_shared<ArrayVector>(
        pool,
        vector->type(),
        arrayVector->nulls(),
        rows.end(),
        arrayVector->offsets(),
        arrayVector->sizes(),
        elementsDict,
        arrayVector->getNullCount());
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    return {
        // array(T) -> array(T)
        exec::FunctionSignatureBuilder()
            .typeVariable("T")
            .returnType("array(T)")
            .argumentType("array(T)")
            .build(),
        // varchar -> varchar
        exec::FunctionSignatureBuilder()
            .returnType("varchar")
            .argumentType("varchar")
            .build(),
    };
  }
};

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_reverse,
    ReverseFunction::signatures(),
    std::make_unique<ReverseFunction>());

} // namespace facebook::velox::functions
