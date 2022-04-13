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

#include "velox/exec/Spill.h"
#include "velox/common/file/FileSystems.h"

namespace facebook::velox::exec {

std::atomic<int32_t> SpillStream::ordinalCounter_;

void SpillInput::next(bool /*throwIfPastEnd*/) {
  int32_t readBytes = std::min(input_->size() - offset_, buffer_->capacity());
  VELOX_CHECK_LT(0, readBytes, "Reading past end of spill file");
  setRange({buffer_->asMutable<uint8_t>(), readBytes, 0});
  input_->pread(offset_, readBytes, buffer_->asMutable<char>());
  offset_ += readBytes;
}

void SpillStream::pop() {
  if (++index_ >= size_) {
    setNextBatch();
  }
}

SpillFile::~SpillFile() {
  try {
    auto fs = filesystems::getFileSystem(path_, nullptr);
    fs->remove(path_);
  } catch (const std::exception& e) {
    LOG(ERROR) << "Error deleting spill file " << path_ << " : " << e.what();
  }
}

WriteFile& SpillFile::output() {
  if (!output_) {
    auto fs = filesystems::getFileSystem(path_, nullptr);
    output_ = fs->openFileForWrite(path_);
  }
  return *output_;
}

void SpillFile::startRead() {
  constexpr uint64_t kMaxReadBufferSize = 1 << 20; // 1MB
  VELOX_CHECK(!output_);
  VELOX_CHECK(!input_);
  auto fs = filesystems::getFileSystem(path_, nullptr);
  auto file = fs->openFileForRead(path_);
  auto buffer = AlignedBuffer::allocate<char>(
      std::min<uint64_t>(fileSize_, kMaxReadBufferSize), &pool_);
  input_ = std::make_unique<SpillInput>(std::move(file), std::move(buffer));
  nextBatch();
}

void SpillFile::nextBatch() {
  index_ = 0;
  if (input_->atEnd()) {
    size_ = 0;
    return;
  }
  VectorStreamGroup::read(input_.get(), &pool_, type_, &rowVector_);
  size_ = rowVector_->size();
}

WriteFile& SpillFileList::currentOutput() {
  if (files_.empty() || !files_.back()->isWritable() ||
      files_.back()->size() > targetFileSize_ * 1.5) {
    if (!files_.empty() && files_.back()->isWritable()) {
      files_.back()->finishWrite();
    }
    files_.push_back(std::make_unique<SpillFile>(
        type_,
        numSortingKeys_,
        fmt::format("{}-{}", path_, files_.size()),
        pool_));
  }
  return files_.back()->output();
}

void SpillFileList::flush() {
  if (batch_) {
    IOBufOutputStream out(
        mappedMemory_, nullptr, std::max<int64_t>(64 * 1024, batch_->size()));
    batch_->flush(&out);
    batch_.reset();
    auto iobuf = out.getIOBuf();
    auto& file = currentOutput();
    for (auto& range : *iobuf) {
      file.append(std::string_view(
          reinterpret_cast<const char*>(range.data()), range.size()));
    }
  }
}

void SpillFileList::write(
    const RowVectorPtr& rows,
    const folly::Range<IndexRange*>& indices) {
  if (!batch_) {
    batch_ = std::make_unique<VectorStreamGroup>(&mappedMemory_);
    batch_->createStreamTree(
        std::static_pointer_cast<const RowType>(rows->type()), 1000);
  }
  batch_->append(rows, indices);

  flush();
}

void SpillFileList::finishFile() {
  flush();
  if (files_.empty()) {
    return;
  }
  if (files_.back()->isWritable()) {
    files_.back()->finishWrite();
  }
}

int64_t SpillFileList::spilledBytes() const {
  int64_t bytes = 0;
  for (auto& file : files_) {
    bytes += file->size();
  }
  return bytes;
}

void SpillState::setNumPartitions(int32_t numPartitions) {
  VELOX_CHECK_LE(numPartitions, maxPartitions());
  VELOX_CHECK_GT(numPartitions, numPartitions_, "May only add partitions");
  numPartitions_ = numPartitions;
}

void SpillState::appendToPartition(
    int32_t partition,
    const RowVectorPtr& rows) {
  // Ensure that partition exist before writing.
  if (!files_.at(partition)) {
    files_[partition] = std::make_unique<SpillFileList>(
        std::static_pointer_cast<const RowType>(rows->type()),
        numSortingKeys_,
        fmt::format("{}-spill-{}", path_, partition),
        targetFileSize_,
        pool_,
        mappedMemory_);
  }

  IndexRange range{0, rows->size()};
  files_[partition]->write(rows, folly::Range<IndexRange*>(&range, 1));
}

std::unique_ptr<TreeOfLosers<SpillStream>> SpillState::startMerge(
    int32_t partition,
    std::unique_ptr<SpillStream>&& extra) {
  VELOX_CHECK_LT(partition, files_.size());
  auto list = std::move(files_[partition]);
  auto files = list->files();
  std::vector<std::unique_ptr<SpillStream>> result;
  for (auto& file : files) {
    file->startRead();
    result.push_back(std::move(file));
  }
  if (extra) {
    result.push_back(std::move(extra));
  }
  return std::make_unique<TreeOfLosers<SpillStream>>(std::move(result));
}

int64_t SpillState::spilledBytes() const {
  int64_t bytes = 0;
  for (auto& list : files_) {
    if (list) {
      bytes += list->spilledBytes();
    }
  }
  return bytes;
}

} // namespace facebook::velox::exec
