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
#include "velox/expression/Expr.h"
#include "velox/expression/VectorFunction.h"

namespace facebook::velox::functions {
namespace {

class MapFunction : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      exec::Expr* caller,
      exec::EvalCtx* context,
      VectorPtr* result) const override {
    VELOX_CHECK(args.size() == 2);

    auto keys = args[0];
    auto values = args[1];

    exec::LocalDecodedVector keysHolder(context, *keys, rows);
    auto decodedKeys = keysHolder.get();

    exec::LocalDecodedVector valuesHolder(context, *values, rows);
    auto decodedValues = valuesHolder.get();

    static const char* kArrayLengthsMismatch =
        "Key and value arrays must be the same length";
    static const char* kDuplicateKey = "Duplicate map keys are not allowed";

    MapVectorPtr mapVector;
    if (decodedKeys->isIdentityMapping() &&
        decodedValues->isIdentityMapping()) {
      auto keysArray = keys->as<ArrayVector>();
      auto valuesArray = values->as<ArrayVector>();

      // Check array lengths
      rows.applyToSelected([&](vector_size_t row) {
        VELOX_USER_CHECK(
            keysArray->sizeAt(row) == valuesArray->sizeAt(row),
            "{}",
            kArrayLengthsMismatch);
      });

      mapVector = std::make_shared<MapVector>(
          context->pool(),
          caller->type(),
          BufferPtr(nullptr),
          rows.size(),
          keysArray->offsets(),
          keysArray->sizes(),
          keysArray->elements(),
          valuesArray->elements(),
          folly::none);
    } else {
      auto keyIndices = decodedKeys->indices();
      auto valueIndices = decodedValues->indices();

      auto keysArray = decodedKeys->base()->as<ArrayVector>();
      auto valuesArray = decodedValues->base()->as<ArrayVector>();

      // Check array lengths
      rows.applyToSelected([&](vector_size_t row) {
        VELOX_USER_CHECK(
            keysArray->sizeAt(keyIndices[row]) ==
                valuesArray->sizeAt(valueIndices[row]),
            "{}",
            kArrayLengthsMismatch);
      });

      BufferPtr offsets = AlignedBuffer::allocate<vector_size_t>(
          rows.size(), context->pool(), 0);
      auto rawOffsets = offsets->asMutable<vector_size_t>();

      BufferPtr sizes = AlignedBuffer::allocate<vector_size_t>(
          rows.size(), context->pool(), 0);
      auto rawSizes = sizes->asMutable<vector_size_t>();

      BufferPtr valuesIndices = AlignedBuffer::allocate<vector_size_t>(
          keysArray->elements()->size(), context->pool(), 0);
      auto rawValuesIndices = valuesIndices->asMutable<vector_size_t>();

      rows.applyToSelected([&](vector_size_t row) {
        auto offset = keysArray->offsetAt(keyIndices[row]);
        auto size = keysArray->sizeAt(keyIndices[row]);
        rawOffsets[row] = offset;
        rawSizes[row] = size;

        auto valuesOffset = valuesArray->offsetAt(valueIndices[row]);
        for (vector_size_t i = 0; i < size; i++) {
          rawValuesIndices[offset + i] = valuesOffset + i;
        }
      });

      auto wrappedValues = BaseVector::wrapInDictionary(
          BufferPtr(nullptr),
          valuesIndices,
          valuesArray->elements()->size(),
          valuesArray->elements());

      mapVector = std::make_shared<MapVector>(
          context->pool(),
          caller->type(),
          BufferPtr(nullptr),
          rows.size(),
          offsets,
          sizes,
          keysArray->elements(),
          wrappedValues,
          folly::none);
    }

    mapVector->canonicalize();

    auto offsets = mapVector->rawOffsets();
    auto sizes = mapVector->rawSizes();
    auto mapKeys = mapVector->mapKeys();

    // Check for duplicate keys
    rows.applyToSelected([&](vector_size_t row) {
      auto offset = offsets[row];
      auto size = sizes[row];
      for (vector_size_t i = 1; i < size; i++) {
        if (mapKeys->equalValueAt(mapKeys.get(), offset + i, offset + i - 1)) {
          VELOX_USER_CHECK(false, "{}", kDuplicateKey);
        }
      }
    });

    context->moveOrCopyResult(mapVector, rows, result);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // array(K), array(V) -> map(K,V)
    return {exec::FunctionSignatureBuilder()
                .typeVariable("K")
                .typeVariable("V")
                .returnType("map(K,V)")
                .argumentType("array(K)")
                .argumentType("array(V)")
                .build()};
  }
};
} // namespace

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_map,
    MapFunction::signatures(),
    std::make_unique<MapFunction>());
} // namespace facebook::velox::functions
