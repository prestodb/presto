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

#include "presto_cpp/main/operators/StorageBasedBroadcastShuffle.h"

using namespace facebook::velox::exec;
using namespace facebook::velox;

namespace facebook::presto::operators {

const std::string StorageBasedBroadcastShuffle::kBroadcastFileExtension =
    ".bin";
const std::string StorageBasedBroadcastShuffle::kBroadcastTempFileExtension =
    ".dot";

void StorageBasedBroadcastShuffle::writeColumnar(
    std::string_view data,
    std::optional<std::string> writerId,
    std::optional<int32_t> partition) {
  if (!outputFilename_.has_value()) {
    VELOX_CHECK(
        broadcastFolders_.has_value() && writerIndex_.has_value() ||
            writerId.has_value(),
        "Not all the fields provided for broadcast write.");
    outputFilename_ =
        broadcastFolders_.value()[writerIndex_.value()] + writerId.value();
  }
  if (!outputFile_) {
    auto fs = velox::filesystems::getFileSystem(outputFilename_.value(), {});
    outputFile_ = std::move(fs->openFileForWrite(outputFilename_.value()));
    VELOX_CHECK_NOT_NULL(
        outputFile_, "Unable to open the broadcast file {}.", outputFilename_.value());
  }
  // Write the page size followed by its binary content.
  int32_t size = data.size();
  outputFile_->append(std::string_view((char*)&size, sizeof(size)));
  outputFile_->append(data);
}

void StorageBasedBroadcastShuffle::noMoreData(bool success) {
  VELOX_CHECK(outputFile_);
  outputFile_->flush();
  outputFile_->close();
}

bool StorageBasedBroadcastShuffle::hasNext(int32_t partition) const {
  return currentFileIndex_ < inputFilenames_.size();
}

velox::BufferPtr StorageBasedBroadcastShuffle::next(
    int32_t partition,
    bool success) {
  // Read the page size and the page itself from the current file and then add
  // the page into the queue.
  int32_t pageSize;
  if (currentFileOffset_ == 0) {
    currentReadFile_ =
        fileSystem_->openFileForRead(inputFilenames_[currentFileIndex_]);
  }
  currentReadFile_->pread(currentFileOffset_, sizeof(pageSize), &pageSize);
  auto buffer = AlignedBuffer::allocate<char>(pageSize, pool_, 0);
  currentFileOffset_ += pageSize;
  currentReadFile_->pread(0, pageSize, buffer->asMutable<void>());

  if (currentFileOffset_ >= currentReadFile_->size()) {
    // Switch to the next broadcast file.
    currentFileIndex_++;
    currentFileOffset_ = 0;
  }

  return buffer;
}
} // namespace facebook::presto::operators