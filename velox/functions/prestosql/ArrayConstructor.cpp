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
#include "velox/functions/prestosql/ArrayConstructor.h"
#include "velox/expression/Expr.h"
#include "velox/expression/VectorFunction.h"

namespace facebook::velox::functions {
namespace {

class ArrayConstructor : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    auto numArgs = args.size();

    context.ensureWritable(rows, outputType, result);
    result->clearNulls(rows);
    auto arrayResult = result->as<ArrayVector>();
    auto sizes = arrayResult->mutableSizes(rows.end());
    auto rawSizes = sizes->asMutable<int32_t>();
    auto offsets = arrayResult->mutableOffsets(rows.end());
    auto rawOffsets = offsets->asMutable<int32_t>();

    auto elementsResult = arrayResult->elements();

    // append to the end of the "elements" vector
    auto baseOffset = elementsResult->size();

    if (args.empty()) {
      rows.applyToSelected([&](vector_size_t row) {
        rawSizes[row] = 0;
        rawOffsets[row] = baseOffset;
      });
    } else {
      elementsResult->resize(baseOffset + numArgs * rows.countSelected());

      if (shouldCopyRanges(elementsResult->type())) {
        std::vector<BaseVector::CopyRange> ranges;
        ranges.reserve(rows.end());

        vector_size_t offset = baseOffset;
        rows.applyToSelected([&](vector_size_t row) {
          rawSizes[row] = numArgs;
          rawOffsets[row] = offset;
          ranges.push_back({row, offset, 1});
          offset += numArgs;
        });

        elementsResult->copyRanges(args[0].get(), ranges);

        for (int i = 1; i < numArgs; i++) {
          for (auto& range : ranges) {
            ++range.targetIndex;
          }
          elementsResult->copyRanges(args[i].get(), ranges);
        }
      } else {
        SelectivityVector targetRows(elementsResult->size(), false);
        std::vector<vector_size_t> toSourceRow(elementsResult->size());

        vector_size_t offset = baseOffset;
        rows.applyToSelected([&](vector_size_t row) {
          rawSizes[row] = numArgs;
          rawOffsets[row] = offset;

          targetRows.setValid(offset, true);
          toSourceRow[offset] = row;

          offset += numArgs;
        });
        targetRows.updateBounds();
        elementsResult->copy(args[0].get(), targetRows, toSourceRow.data());

        for (int i = 1; i < numArgs; i++) {
          targetRows.clearAll();

          vector_size_t offset_2 = baseOffset;
          rows.applyToSelected([&](vector_size_t row) {
            targetRows.setValid(offset_2 + i, true);
            toSourceRow[offset_2 + i] = row;
            offset_2 += numArgs;
          });

          targetRows.updateBounds();
          elementsResult->copy(args[i].get(), targetRows, toSourceRow.data());
        }
      }
    }
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    return {
        // () -> array(unknown)
        exec::FunctionSignatureBuilder().returnType("array(unknown)").build(),
        // T... -> array(T)
        exec::FunctionSignatureBuilder()
            .typeVariable("T")
            .returnType("array(T)")
            .argumentType("T")
            .variableArity()
            .build(),
    };
  }

 private:
  // BaseVector::copyRange is faster for arrays and maps and slower for
  // primitive types. Check if 'type' is an array or map or contains an array or
  // map. If so, return true, otherwise, false.
  static bool shouldCopyRanges(const TypePtr& type) {
    if (type->isPrimitiveType()) {
      return false;
    }

    if (!type->isRow()) {
      return true;
    }

    const auto& rowType = type->asRow();
    for (const auto& child : rowType.children()) {
      if (shouldCopyRanges(child)) {
        return true;
      }
    }
    return false;
  }
};
} // namespace

VELOX_DECLARE_VECTOR_FUNCTION_WITH_METADATA(
    udf_array_constructor,
    ArrayConstructor::signatures(),
    exec::VectorFunctionMetadataBuilder().defaultNullBehavior(false).build(),
    std::make_unique<ArrayConstructor>());

void registerArrayConstructor(const std::string& name) {
  VELOX_REGISTER_VECTOR_FUNCTION(udf_array_constructor, name);
}

} // namespace facebook::velox::functions
