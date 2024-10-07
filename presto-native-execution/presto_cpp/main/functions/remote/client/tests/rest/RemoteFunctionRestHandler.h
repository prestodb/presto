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
#pragma once

#include <folly/io/IOBuf.h>

#include "velox/vector/VectorStream.h"

namespace facebook::presto::functions::rest {

class RemoteFunctionRestHandler {
 public:
  RemoteFunctionRestHandler(
      velox::RowTypePtr inputTypes,
      velox::TypePtr outputType)
      : inputTypes_{std::move(inputTypes)},
        outputType_{std::move(outputType)} {}

  virtual ~RemoteFunctionRestHandler() = default;

  folly::IOBuf handleRequest(
      std::unique_ptr<folly::IOBuf> inputBuffer,
      velox::VectorSerde* serde,
      velox::memory::MemoryPool* pool,
      std::string& errorMessage) {
    auto inputVector =
        IOBufToRowVector(*inputBuffer, inputTypes_, *pool, serde);

    VELOX_CHECK_EQ(
        inputVector->childrenSize(),
        inputTypes_->children().size(),
        "Mismatched number of columns for remote function handler.");

    const auto numRows = inputVector->size();
    auto resultVector = velox::BaseVector::create(outputType_, numRows, pool);

    compute(inputVector, resultVector, errorMessage);

    if (!errorMessage.empty()) {
      return folly::IOBuf();
    }

    // Wrap the result in a RowVector to send back.
    auto outputRowVector = std::make_shared<velox::RowVector>(
        pool,
        velox::ROW({outputType_}),
        velox::BufferPtr(),
        numRows,
        std::vector<velox::VectorPtr>{resultVector});

    auto payload = rowVectorToIOBuf(
        outputRowVector, outputRowVector->size(), *pool, serde);

    return payload;
  }

 protected:
  // Core computation function to be implemented by subclasses.
  virtual void compute(
      const velox::RowVectorPtr& inputVector,
      const velox::VectorPtr& resultVector,
      std::string& errorMessage) = 0;

 private:
  const velox::RowTypePtr inputTypes_;
  const velox::TypePtr outputType_;
};

} // namespace facebook::presto::functions::rest
