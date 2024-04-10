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
#include "velox/exec/ArrowStream.h"
#include "velox/vector/arrow/Bridge.h"

namespace facebook::velox::exec {

ArrowStream::ArrowStream(
    int32_t operatorId,
    DriverCtx* driverCtx,
    const std::shared_ptr<const core::ArrowStreamNode>& arrowStreamNode)
    : SourceOperator(
          driverCtx,
          arrowStreamNode->outputType(),
          operatorId,
          arrowStreamNode->id(),
          "ArrowStream") {
  arrowStream_ = arrowStreamNode->arrowStream();
}

ArrowStream::~ArrowStream() {
  close();
}

RowVectorPtr ArrowStream::getOutput() {
  // Get Arrow array.
  struct ArrowArray arrowArray;
  if (arrowStream_->get_next(arrowStream_.get(), &arrowArray)) {
    if (arrowArray.release) {
      arrowArray.release(&arrowArray);
    }
    VELOX_FAIL(
        "Failed to call get_next on ArrowStream: {}", std::string(getError()));
  }
  if (arrowArray.release == nullptr) {
    // End of Stream.
    finished_ = true;
    return nullptr;
  }

  // Get Arrow schema.
  struct ArrowSchema arrowSchema;
  if (arrowStream_->get_schema(arrowStream_.get(), &arrowSchema)) {
    if (arrowSchema.release) {
      arrowSchema.release(&arrowSchema);
    }
    if (arrowArray.release) {
      arrowArray.release(&arrowArray);
    }
    VELOX_FAIL(
        "Failed to call get_schema on ArrowStream: {}",
        std::string(getError()));
  }

  // Convert Arrow Array into RowVector and return.
  return std::dynamic_pointer_cast<RowVector>(
      importFromArrowAsOwner(arrowSchema, arrowArray, pool()));
}

bool ArrowStream::isFinished() {
  return finished_;
}

const char* ArrowStream::getError() const {
  const char* lastError = arrowStream_->get_last_error(arrowStream_.get());
  VELOX_CHECK_NOT_NULL(lastError);
  return lastError;
}

void ArrowStream::close() {
  if (arrowStream_->release) {
    arrowStream_->release(arrowStream_.get());
  }
  SourceOperator::close();
}

} // namespace facebook::velox::exec
