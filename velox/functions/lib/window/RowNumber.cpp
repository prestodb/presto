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

#include "velox/common/base/Exceptions.h"
#include "velox/exec/WindowFunction.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::functions::window {

namespace {

class RowNumberFunction : public exec::WindowFunction {
 public:
  explicit RowNumberFunction(const TypePtr& resultType)
      : WindowFunction(resultType, nullptr, nullptr) {}

  void resetPartition(const exec::WindowPartition* /*partition*/) override {
    rowNumber_ = 1;
  }

  void apply(
      const BufferPtr& peerGroupStarts,
      const BufferPtr& /*peerGroupEnds*/,
      const BufferPtr& /*frameStarts*/,
      const BufferPtr& /*frameEnds*/,
      const SelectivityVector& validRows,
      vector_size_t resultOffset,
      const VectorPtr& result) override {
    if (resultType_->isInteger()) {
      applyTyped<int32_t>(peerGroupStarts, resultOffset, result);
    } else {
      applyTyped<int64_t>(peerGroupStarts, resultOffset, result);
    }
  }

  template <typename T>
  void applyTyped(
      const BufferPtr& peerGroupStarts,
      vector_size_t resultOffset,
      const VectorPtr& result) {
    int numRows = peerGroupStarts->size() / sizeof(vector_size_t);
    auto* rawValues = result->asFlatVector<T>()->mutableRawValues();
    for (int i = 0; i < numRows; i++) {
      rawValues[resultOffset + i] = rowNumber_++;
    }
  }

 private:
  int64_t rowNumber_ = 1;
};

} // namespace

void registerRowNumber(const std::string& name, TypeKind resultTypeKind) {
  std::vector<exec::FunctionSignaturePtr> signatures{
      exec::FunctionSignatureBuilder()
          .returnType(mapTypeKindToName(resultTypeKind))
          .build(),
  };

  exec::registerWindowFunction(
      name,
      std::move(signatures),
      {exec::WindowFunction::ProcessMode::kRows, false},
      [name](
          const std::vector<exec::WindowFunctionArg>& /*args*/,
          const TypePtr& resultType,
          bool /*ignoreNulls*/,
          velox::memory::MemoryPool* /*pool*/,
          HashStringAllocator* /*stringAllocator*/,
          const core::QueryConfig& /*queryConfig*/)
          -> std::unique_ptr<exec::WindowFunction> {
        return std::make_unique<RowNumberFunction>(resultType);
      });
}

void registerRowNumberInteger(const std::string& name) {
  registerRowNumber(name, TypeKind::INTEGER);
}

void registerRowNumberBigint(const std::string& name) {
  registerRowNumber(name, TypeKind::BIGINT);
}
} // namespace facebook::velox::functions::window
