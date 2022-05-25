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
#include "velox/expression/VectorFunction.h"
#include "velox/functions/lib/LambdaFunctionUtil.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/NullsBuilder.h"

namespace facebook::velox::functions {
namespace {

template <template <typename> class F, TypeKind kind>
VectorPtr applyTyped(
    const SelectivityVector& rows,
    const ArrayVector& arrayVector,
    DecodedVector& elementsDecoded,
    exec::EvalCtx* context) {
  auto pool = context->pool();
  using T = typename TypeTraits<kind>::NativeType;

  auto rawSizes = arrayVector.rawSizes();
  auto rawOffsets = arrayVector.rawOffsets();

  BufferPtr indices = allocateIndices(rows.size(), pool);
  auto rawIndices = indices->asMutable<vector_size_t>();

  // Create nulls for lazy initialization.
  NullsBuilder nullsBuilder(rows.size(), pool);

  if (elementsDecoded.isIdentityMapping() && !elementsDecoded.mayHaveNulls()) {
    if constexpr (std::is_same_v<bool, T>) {
      auto rawElements = elementsDecoded.data<uint64_t>();
      rows.applyToSelected([&](auto row) {
        auto size = rawSizes[row];
        if (size == 0) {
          nullsBuilder.setNull(row);
        } else {
          auto offset = rawOffsets[row];
          auto elementIndex = offset;
          for (auto i = offset + 1; i < offset + size; i++) {
            if (F<T>()(
                    bits::isBitSet(rawElements, i),
                    bits::isBitSet(rawElements, elementIndex))) {
              elementIndex = i;
            }
          }
          rawIndices[row] = elementIndex;
        }
      });
    } else {
      auto rawElements = elementsDecoded.data<T>();
      rows.applyToSelected([&](auto row) {
        auto size = rawSizes[row];
        if (size == 0) {
          nullsBuilder.setNull(row);
        } else {
          auto offset = rawOffsets[row];
          auto elementIndex = offset;
          for (auto i = offset + 1; i < offset + size; i++) {
            if (F<T>()(rawElements[i], rawElements[elementIndex])) {
              elementIndex = i;
            }
          }
          rawIndices[row] = elementIndex;
        }
      });
    }
  } else {
    rows.applyToSelected([&](auto row) {
      auto size = rawSizes[row];
      if (size == 0) {
        nullsBuilder.setNull(row);
      } else {
        auto offset = rawOffsets[row];
        auto elementIndex = offset;
        for (auto i = offset; i < offset + size; i++) {
          if (elementsDecoded.isNullAt(i)) {
            // If a NULL value is encountered, min/max are always NULL
            nullsBuilder.setNull(row);
            break;
          } else if (F<T>()(
                         elementsDecoded.valueAt<T>(i),
                         elementsDecoded.valueAt<T>(elementIndex))) {
            elementIndex = i;
          }
        }
        rawIndices[row] = elementIndex;
      }
    });
  }

  return BaseVector::wrapInDictionary(
      nullsBuilder.build(), indices, rows.size(), arrayVector.elements());
}

template <template <typename> class F>
class ArrayMinMaxFunction : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx* context,
      VectorPtr* result) const override {
    VELOX_CHECK_EQ(args.size(), 1);
    auto arrayVector = args[0]->asUnchecked<ArrayVector>();

    auto elementsVector = arrayVector->elements();
    exec::LocalSelectivityVector elementsRows(context, elementsVector->size());
    exec::LocalDecodedVector elementsHolder(
        context, *elementsVector, *elementsRows.get());
    auto localResult = VELOX_DYNAMIC_SCALAR_TEMPLATE_TYPE_DISPATCH(
        applyTyped,
        F,
        elementsVector->typeKind(),
        rows,
        *arrayVector,
        *elementsHolder.get(),
        context);
    context->moveOrCopyResult(localResult, rows, result);
  }
};

std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
  static const std::vector<std::string> kSupportedTypeNames = {
      "boolean",
      "tinyint",
      "smallint",
      "integer",
      "bigint",
      "real",
      "double",
      "varchar",
      "timestamp"};

  std::vector<std::shared_ptr<exec::FunctionSignature>> signatures;
  signatures.reserve(kSupportedTypeNames.size());
  for (const auto& typeName : kSupportedTypeNames) {
    signatures.emplace_back(
        exec::FunctionSignatureBuilder()
            .returnType(typeName)
            .argumentType(fmt::format("array({})", typeName))
            .build());
  }
  return signatures;
}
} // namespace

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_array_min,
    signatures(),
    std::make_unique<ArrayMinMaxFunction<std::less>>());

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_array_max,
    signatures(),
    std::make_unique<ArrayMinMaxFunction<std::greater>>());

} // namespace facebook::velox::functions
