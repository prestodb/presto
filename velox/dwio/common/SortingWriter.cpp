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

#include "velox/dwio/common/SortingWriter.h"

namespace facebook::velox::dwio::common {

SortingWriter::SortingWriter(
    std::unique_ptr<Writer> writer,
    std::unique_ptr<exec::SortBuffer> sortBuffer)
    : outputWriter_(std::move(writer)), sortBuffer_(std::move(sortBuffer)) {}

void SortingWriter::write(const VectorPtr& data) {
  sortBuffer_->addInput(data);
}

void SortingWriter::flush() {
  // TODO: add to support flush by disk spilling.
}

void SortingWriter::close() {
  sortBuffer_->noMoreInput();
  RowVectorPtr output = sortBuffer_->getOutput();
  while (output != nullptr) {
    outputWriter_->write(output);
    output = sortBuffer_->getOutput();
  }
  outputWriter_->close();

  sortBuffer_->pool()->release();
}

void SortingWriter::abort() {
  sortBuffer_.reset();
  outputWriter_->abort();
}

} // namespace facebook::velox::dwio::common
