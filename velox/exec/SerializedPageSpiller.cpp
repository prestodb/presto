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

#include "velox/exec/SerializedPageSpiller.h"
#include "velox/exec/SpillFile.h"

namespace facebook::velox::exec {

void SerializedPageSpiller::spill(
    const std::vector<std::shared_ptr<SerializedPage>>& pages) {
  writeWithBufferControl([&]() {
    if (pages.empty()) {
      return 0UL;
    }

    if (bufferStream_ == nullptr) {
      bufferStream_ = std::make_unique<IOBufOutputStream>(*pool_);
    }

    // Spill file layout:
    //  --- Page 0 ---
    //
    //  (1 Byte) is null page
    //  (8 Bytes) payload size
    //  (1 Bytes) has num rows
    //  (8 Bytes) num rows
    //  (x Bytes) payload
    //
    //  --- Page 1 ---
    //        ...
    //  --- Page n ---
    //        ...
    const auto totalBytesBeforeSpill = totalBytes_;
    uint64_t totalRows{0};
    for (const auto& page : pages) {
      if (page != nullptr) {
        const auto pageSize = page->size();
        totalBytes_ += pageSize;
        totalRows += page->numRows().value_or(0);
      }

      // Spill payload headers.
      const uint8_t isNull = (page == nullptr) ? 1 : 0;
      bufferStream_->write(
          reinterpret_cast<const char*>(&isNull), sizeof(uint8_t));
      if (page == nullptr) {
        continue;
      }

      auto pageBytes = static_cast<int64_t>(page->size());
      bufferStream_->write(
          reinterpret_cast<char*>(&pageBytes), sizeof(int64_t));

      const auto numRowsOpt = page->numRows();
      uint8_t hasNumRows = numRowsOpt.has_value() ? 1 : 0;
      bufferStream_->write(reinterpret_cast<const char*>(&hasNumRows), 1);
      if (numRowsOpt.has_value()) {
        const int64_t numRows = numRowsOpt.value();
        bufferStream_->write(
            reinterpret_cast<const char*>(&numRows), sizeof(int64_t));
      }

      // Spill payload.
      auto iobuf = page->getIOBuf();
      for (auto range = iobuf->begin(); range != iobuf->end(); ++range) {
        bufferStream_->write(
            reinterpret_cast<const char*>(range->data()),
            static_cast<int64_t>(range->size()));
      }
    }

    VELOX_CHECK_GE(totalBytes_, totalBytesBeforeSpill);
    totalPages_ += pages.size();
    return totalRows;
  });
}

SerializedPageSpiller::Result SerializedPageSpiller::finishSpill() {
  auto spillFiles = finish();
  return {std::move(spillFiles), totalPages_};
}

void SerializedPageSpiller::flushBuffer(
    SpillWriteFile* file,
    uint64_t& writtenBytes,
    uint64_t& flushTimeNs,
    uint64_t& writeTimeNs) {
  flushTimeNs = 0;
  {
    NanosecondTimer timer(&writeTimeNs);
    writtenBytes = file->write(bufferStream_->getIOBuf());
  }
}

bool SerializedPageSpiller::bufferEmpty() const {
  return bufferStream_ == nullptr;
}

uint64_t SerializedPageSpiller::bufferSize() const {
  return bufferStream_ == nullptr
      ? 0
      : static_cast<uint64_t>(bufferStream_->tellp());
}

void SerializedPageSpiller::addFinishedFile(SpillWriteFile* file) {
  SpillFileInfo spillFileInfo;
  spillFileInfo.path = file->path();
  finishedFiles_.push_back(std::move(spillFileInfo));
}

bool SerializedPageSpillReader::empty() const {
  return numPages_ == 0;
}

uint64_t SerializedPageSpillReader::numPages() const {
  return numPages_;
}

std::shared_ptr<SerializedPage> SerializedPageSpillReader::at(uint64_t index) {
  ensurePages(index);
  return bufferedPages_[index];
}

void SerializedPageSpillReader::deleteAll() {
  spillFilePaths_.clear();
  curFileStream_.reset();
  bufferedPages_.clear();
  numPages_ = 0;
}

void SerializedPageSpillReader::deleteFront(uint64_t numPages) {
  if (numPages == 0) {
    return;
  }
  ensurePages(numPages - 1);
  bufferedPages_.erase(
      bufferedPages_.begin(), bufferedPages_.begin() + numPages);
  numPages_ -= numPages;
}

void SerializedPageSpillReader::ensureSpillFile() {
  if (curFileStream_ != nullptr) {
    return;
  }
  VELOX_CHECK(!spillFilePaths_.empty());
  auto filePath = spillFilePaths_.front();
  auto fs = filesystems::getFileSystem(filePath, nullptr);
  auto file = fs->openFileForRead(filePath);
  curFileStream_ = std::make_unique<common::FileInputStream>(
      std::move(file), readBufferSize_, pool_);
  spillFilePaths_.pop_front();
}

void SerializedPageSpillReader::ensurePages(uint64_t index) {
  VELOX_CHECK_LT(index, numPages_);
  if (index < bufferedPages_.size()) {
    return;
  }

  while (index >= bufferedPages_.size()) {
    bufferedPages_.push_back(unspillNextPage());
  }
}

namespace {
struct FreeData {
  const std::shared_ptr<memory::MemoryPool> pool;
  int64_t bytesToFree{0};
};

void freeFunc(void* data, void* userData) {
  auto* freeData = reinterpret_cast<FreeData*>(userData);
  freeData->pool->free(data, freeData->bytesToFree);
  delete freeData;
}
} // namespace

std::shared_ptr<SerializedPage> SerializedPageSpillReader::unspillNextPage() {
  VELOX_CHECK(!empty());
  ensureSpillFile();

  SCOPE_EXIT {
    if (curFileStream_->atEnd()) {
      curFileStream_.reset();
    }
  };

  // Read payload headers
  const auto isNull = !!(curFileStream_->read<uint8_t>());
  if (isNull) {
    return nullptr;
  }
  const auto iobufBytes = curFileStream_->read<int64_t>();
  const auto hasNumRows = curFileStream_->read<uint8_t>() == 0 ? false : true;
  int64_t numRows{0};
  if (hasNumRows) {
    numRows = curFileStream_->read<int64_t>();
  }

  // Read payload
  VELOX_CHECK_GE(curFileStream_->remainingSize(), iobufBytes);
  void* rawBuf = pool_->allocate(iobufBytes);
  curFileStream_->readBytes(reinterpret_cast<uint8_t*>(rawBuf), iobufBytes);

  auto* userData = new FreeData{pool_->shared_from_this(), iobufBytes};
  auto iobuf =
      folly::IOBuf::takeOwnership(rawBuf, iobufBytes, freeFunc, userData, true);

  return std::make_shared<SerializedPage>(
      std::move(iobuf),
      nullptr,
      hasNumRows ? std::optional(numRows) : std::nullopt);
}
} // namespace facebook::velox::exec
