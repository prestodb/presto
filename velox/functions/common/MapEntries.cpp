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

// See documentation at https://prestodb.io/docs/current/functions/map.html
class MapEntriesFunction : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      exec::Expr* caller,
      exec::EvalCtx* context,
      VectorPtr* result) const override {
    VELOX_CHECK_EQ(args.size(), 1);
    VELOX_CHECK_EQ(args[0]->type()->kind(), TypeKind::MAP);

    const auto inputMap = args[0]->as<MapVector>();

    VectorPtr resultElements = std::make_shared<RowVector>(
        context->pool(),
        caller->type()->childAt(0),
        BufferPtr(nullptr),
        inputMap->mapKeys()->size(),
        std::vector<VectorPtr>{inputMap->mapKeys(), inputMap->mapValues()},
        folly::none);
    auto resultArray = std::make_shared<ArrayVector>(
        context->pool(),
        caller->type(),
        inputMap->nulls(),
        rows.size(),
        inputMap->offsets(),
        inputMap->sizes(),
        resultElements,
        folly::none);

    context->moveOrCopyResult(resultArray, rows, result);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // map(K,V) -> array(row(K,V))
    return {exec::FunctionSignatureBuilder()
                .typeVariable("K")
                .typeVariable("V")
                .returnType("array(row(K,V))")
                .argumentType("map(K,V)")
                .build()};
  }
};
} // namespace

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_map_entries,
    MapEntriesFunction::signatures(),
    std::make_unique<MapEntriesFunction>());
} // namespace facebook::velox::functions
