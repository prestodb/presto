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

namespace facebook::velox::window {

namespace {

class RowNumberFunction : public exec::WindowFunction {
 public:
  explicit RowNumberFunction() : WindowFunction(BIGINT(), nullptr) {}

  void resetPartition(const exec::WindowPartition* /*partition*/) override {
    rowNumber_ = 1;
  }

  void apply(
      const BufferPtr& peerGroupStarts,
      const BufferPtr& /*peerGroupEnds*/,
      const BufferPtr& /*frameStarts*/,
      const BufferPtr& /*frameEnds*/,
      vector_size_t resultOffset,
      const VectorPtr& result) override {
    int numRows = peerGroupStarts->size() / sizeof(vector_size_t);
    auto* rawValues = result->asFlatVector<int64_t>()->mutableRawValues();
    for (int i = 0; i < numRows; i++) {
      rawValues[resultOffset + i] = rowNumber_++;
    }
  }

 private:
  int64_t rowNumber_ = 1;
};

} // namespace

// Signature of this function is : row_number() -> bigint.
void registerRowNumber(const std::string& name) {
  std::vector<exec::FunctionSignaturePtr> signatures{
      exec::FunctionSignatureBuilder().returnType("bigint").build(),
  };

  exec::registerWindowFunction(
      name,
      std::move(signatures),
      [name](
          const std::vector<exec::WindowFunctionArg>& /*args*/,
          const TypePtr& /*resultType*/,
          velox::memory::MemoryPool* /*pool*/)
          -> std::unique_ptr<exec::WindowFunction> {
        return std::make_unique<RowNumberFunction>();
      });
}
} // namespace facebook::velox::window
