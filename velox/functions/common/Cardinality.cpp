/*
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

namespace facebook::velox::functions {
namespace {

class CardinalityFunction : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      exec::Expr* /* caller */,
      exec::EvalCtx* context,
      VectorPtr* result) const override {
    VELOX_CHECK(args.size() == 1);
    auto arg = args[0];

    switch (arg->typeKind()) {
      case TypeKind::ARRAY:
        applyTyped(rows, arg->as<ArrayVector>(), context, result);
        return;
      case TypeKind::MAP:
        applyTyped(rows, arg->as<MapVector>(), context, result);
        return;
      default:
        VELOX_CHECK(
            false,
            "Unsupported type for cardinality function {}",
            mapTypeKindToName(arg->typeKind()));
    }
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    return {
        // array(T) -> bigint
        exec::FunctionSignatureBuilder()
            .typeVariable("T")
            .returnType("bigint")
            .argumentType("array(T)")
            .build(),
        // map(K,V) -> bigint
        exec::FunctionSignatureBuilder()
            .typeVariable("K")
            .typeVariable("V")
            .returnType("bigint")
            .argumentType("map(K,V)")
            .build(),
    };
  }

 private:
  template <typename T>
  void applyTyped(
      const SelectivityVector& rows,
      T* arg,
      exec::EvalCtx* context,
      VectorPtr* result) const {
    BaseVector::ensureWritable(rows, BIGINT(), context->pool(), result);
    BufferPtr resultValues =
        (*result)->as<FlatVector<int64_t>>()->mutableValues(rows.size());
    auto rawResult = resultValues->asMutable<int64_t>();

    // Use underlying complex vector's sizeAt() function.
    rows.applyToSelected(
        [&](vector_size_t row) { rawResult[row] = arg->sizeAt(row); });
  }
};
} // namespace

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_cardinality,
    CardinalityFunction::signatures(),
    std::make_unique<CardinalityFunction>());
} // namespace facebook::velox::functions
