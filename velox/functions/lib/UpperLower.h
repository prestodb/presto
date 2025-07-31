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

#include "velox/expression/StringWriter.h"
#include "velox/expression/VectorFunction.h"
#include "velox/functions/lib/StringEncodingUtils.h"
#include "velox/functions/lib/string/StringImpl.h"

namespace facebook::velox::functions {

/// Function to convert string to upper or lower case. Ascii and unicode
/// conversion are supported.
/// @tparam isLower Instantiate for upper or lower.
/// @tparam turkishCasing If true, Spark's specific behavior on Turkish casing
/// is considered. For 'İ' Spark's lower case is 'i̇' and Presto's is 'i'.
/// @tparam greekFinalSigma If true, Greek final sigma rule is applied. For
/// the uppercase letter Σ, if it appears at the end of a word, it becomes ς. In
/// all other positions, it becomes σ. If false, it is always converted to σ.
template <bool isLower, bool turkishCasing, bool greekFinalSigma>
class UpperLowerTemplateFunction : public exec::VectorFunction {
 private:
  // String encoding wrappable function.
  template <bool isAscii>
  struct ApplyInternal {
    static void apply(
        const SelectivityVector& rows,
        const DecodedVector* decodedInput,
        FlatVector<StringView>* results) {
      rows.applyToSelected([&](auto row) {
        auto proxy = exec::StringWriter(results, row);
        if constexpr (isLower) {
          stringImpl::lower<isAscii, turkishCasing, greekFinalSigma>(
              proxy, decodedInput->valueAt<StringView>(row));
        } else {
          stringImpl::upper<isAscii, turkishCasing>(
              proxy, decodedInput->valueAt<StringView>(row));
        }
        proxy.finalize();
      });
    }
  };

 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /*outputType*/,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    VELOX_CHECK(args.size() == 1);
    VELOX_CHECK(args[0]->typeKind() == TypeKind::VARCHAR);

    // Read content before calling prepare results.
    BaseVector* inputStringsVector = args[0].get();
    exec::LocalDecodedVector inputHolder(context, *inputStringsVector, rows);
    auto decodedInput = inputHolder.get();

    auto ascii = isAscii(inputStringsVector, rows);

    // Not in place path.
    VectorPtr emptyVectorPtr;
    prepareFlatResultsVector(result, rows, context, emptyVectorPtr);
    auto* resultFlatVector = result->as<FlatVector<StringView>>();

    StringEncodingTemplateWrapper<ApplyInternal>::apply(
        ascii, rows, decodedInput, resultFlatVector);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // varchar -> varchar
    return {exec::FunctionSignatureBuilder()
                .returnType("varchar")
                .argumentType("varchar")
                .build()};
  }

  bool ensureStringEncodingSetAtAllInputs() const override {
    return true;
  }

  bool propagateStringEncodingFromAllInputs() const override {
    return true;
  }
};

} // namespace facebook::velox::functions
