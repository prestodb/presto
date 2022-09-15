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
#include "presto_cpp/main/operators/StorageBasedExchangeSource.h"

namespace facebook::presto::operators {

void StorageBasedExchangeSource::request() {
  std::lock_guard<std::mutex> l(queue_->mutex());

  if (currentFileIndex_ >= filenames_.size()) {
    atEnd_ = true;
    queue_->enqueue(nullptr);
    return;
  }

  // Read the page size and the page itself from the current file and then add
  // the page into the queue.
  int pageSize;
  auto file = fileSystem_->openFileForRead(filenames_[currentFileIndex_]);
  file->pread(currentFileOffset_, sizeof(pageSize), &pageSize);
  auto page = pool_->allocate(pageSize);
  currentFileOffset_ += sizeof(pageSize);
  file->pread(currentFileOffset_, pageSize, page);
  currentFileOffset_ += pageSize;
  auto ioBuf = folly::IOBuf::wrapBuffer(page, pageSize);
  queue_->enqueue(std::make_unique<velox::exec::SerializedPage>(
      std::move(ioBuf), pool_, [page, pageSize, this](auto&) {
        pool_->free(page, pageSize);
      }));
  if (currentFileOffset_ >= file->size()) {
    // Switch to the next broadcast file.
    currentFileIndex_++;
    currentFileOffset_ = 0;
  }
}
}; // namespace facebook::presto::operators