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

#include "velox/exec/SpillFile.h"
#include "velox/common/base/RuntimeMetrics.h"
#include "velox/common/file/FileSystems.h"

namespace facebook::velox::exec {
namespace {
// Spilling currently uses the default PrestoSerializer which by default
// serializes timestamp with millisecond precision to maintain compatibility
// with presto. Since velox's native timestamp implementation supports
// nanosecond precision, we use this serde option to ensure the serializer
// preserves precision.
static const bool kDefaultUseLosslessTimestamp = true;
} // namespace

SpillInputStream::SpillInputStream(
    std::unique_ptr<ReadFile>&& file,
    uint64_t bufferSize,
    memory::MemoryPool* pool,
    folly::Synchronized<common::SpillStats>* stats)
    : file_(std::move(file)),
      fileSize_(file_->size()),
      bufferSize_(std::min(fileSize_, bufferSize - AlignedBuffer::kPaddedSize)),
      pool_(pool),
      readaEnabled_((bufferSize_ < fileSize_) && file_->hasPreadvAsync()),
      stats_(stats) {
  VELOX_CHECK_NOT_NULL(pool_);
  VELOX_CHECK_GT(
      bufferSize, AlignedBuffer::kPaddedSize, "Buffer size is too small");
  buffers_.push_back(AlignedBuffer::allocate<char>(bufferSize_, pool_));
  if (readaEnabled_) {
    buffers_.push_back(AlignedBuffer::allocate<char>(bufferSize_, pool_));
  }
  next(/*throwIfPastEnd=*/true);
}

SpillInputStream::~SpillInputStream() {
  if (!readaWait_.valid()) {
    return;
  }
  try {
    readaWait_.wait();
  } catch (const std::exception& ex) {
    // ignore any prefetch error when query has failed.
    LOG(WARNING) << "Spill read-ahead failed on destruction " << ex.what();
  }
}

void SpillInputStream::next(bool /*throwIfPastEnd*/) {
  int32_t readBytes{0};
  uint64_t readTimeUs{0};
  if (readaWait_.valid()) {
    {
      MicrosecondTimer timer{&readTimeUs};
      readBytes = std::move(readaWait_)
                      .via(&folly::QueuedImmediateExecutor::instance())
                      .wait()
                      .value();
      VELOX_CHECK(!readaWait_.valid());
    }
    VELOX_CHECK_LT(0, readBytes, "Reading past end of spill file");
    advanceBuffer();
  } else {
    readBytes = readSize();
    VELOX_CHECK_LT(0, readBytes, "Reading past end of spill file");
    {
      MicrosecondTimer timer{&readTimeUs};
      file_->pread(offset_, readBytes, buffer()->asMutable<char>());
    }
  }
  setRange({buffer()->asMutable<uint8_t>(), readBytes, 0});
  updateSpillStats(readBytes, readTimeUs);

  offset_ += readBytes;
  maybeIssueReadahead();
}

uint64_t SpillInputStream::readSize() const {
  return std::min(fileSize_ - offset_, bufferSize_);
}

void SpillInputStream::maybeIssueReadahead() {
  VELOX_CHECK(!readaWait_.valid());
  if (!readaEnabled_) {
    return;
  }
  const auto size = readSize();
  if (size == 0) {
    return;
  }
  std::vector<folly::Range<char*>> ranges;
  ranges.emplace_back(nextBuffer()->asMutable<char>(), size);
  readaWait_ = file_->preadvAsync(offset_, ranges);
  VELOX_CHECK(readaWait_.valid());
}

void SpillInputStream::updateSpillStats(uint64_t readBytes, uint64_t readTimeUs)
    const {
  auto lockedStats = stats_->wlock();
  lockedStats->spillReadBytes += readBytes;
  lockedStats->spillReadTimeUs += readTimeUs;
  ++(lockedStats->spillReads);
  common::updateGlobalSpillReadStats(readBytes, readTimeUs);
}

std::unique_ptr<SpillWriteFile> SpillWriteFile::create(
    uint32_t id,
    const std::string& pathPrefix,
    const std::string& fileCreateConfig) {
  return std::unique_ptr<SpillWriteFile>(
      new SpillWriteFile(id, pathPrefix, fileCreateConfig));
}

SpillWriteFile::SpillWriteFile(
    uint32_t id,
    const std::string& pathPrefix,
    const std::string& fileCreateConfig)
    : id_(id), path_(fmt::format("{}-{}", pathPrefix, ordinalCounter_++)) {
  auto fs = filesystems::getFileSystem(path_, nullptr);
  file_ = fs->openFileForWrite(
      path_,
      filesystems::FileOptions{
          {{filesystems::FileOptions::kFileCreateConfig.toString(),
            fileCreateConfig}},
          nullptr,
          std::nullopt});
}

void SpillWriteFile::finish() {
  VELOX_CHECK_NOT_NULL(file_);
  size_ = file_->size();
  file_->close();
  file_ = nullptr;
}

uint64_t SpillWriteFile::size() const {
  if (file_ != nullptr) {
    return file_->size();
  }
  return size_;
}

uint64_t SpillWriteFile::write(std::unique_ptr<folly::IOBuf> iobuf) {
  auto writtenBytes = iobuf->computeChainDataLength();
  file_->append(std::move(iobuf));
  return writtenBytes;
}

SpillWriter::SpillWriter(
    const RowTypePtr& type,
    const uint32_t numSortKeys,
    const std::vector<CompareFlags>& sortCompareFlags,
    common::CompressionKind compressionKind,
    const std::string& pathPrefix,
    uint64_t targetFileSize,
    uint64_t writeBufferSize,
    const std::string& fileCreateConfig,
    common::UpdateAndCheckSpillLimitCB& updateAndCheckSpillLimitCb,
    memory::MemoryPool* pool,
    folly::Synchronized<common::SpillStats>* stats)
    : type_(type),
      numSortKeys_(numSortKeys),
      sortCompareFlags_(sortCompareFlags),
      compressionKind_(compressionKind),
      pathPrefix_(pathPrefix),
      targetFileSize_(targetFileSize),
      writeBufferSize_(writeBufferSize),
      fileCreateConfig_(fileCreateConfig),
      updateAndCheckSpillLimitCb_(updateAndCheckSpillLimitCb),
      pool_(pool),
      stats_(stats) {
  // NOTE: if the associated spilling operator has specified the sort
  // comparison flags, then it must match the number of sorting keys.
  VELOX_CHECK(
      sortCompareFlags_.empty() || sortCompareFlags_.size() == numSortKeys_);
}

SpillWriteFile* SpillWriter::ensureFile() {
  if ((currentFile_ != nullptr) && (currentFile_->size() > targetFileSize_)) {
    closeFile();
  }
  if (currentFile_ == nullptr) {
    currentFile_ = SpillWriteFile::create(
        nextFileId_++,
        fmt::format("{}-{}", pathPrefix_, finishedFiles_.size()),
        fileCreateConfig_);
  }
  return currentFile_.get();
}

void SpillWriter::closeFile() {
  if (currentFile_ == nullptr) {
    return;
  }
  currentFile_->finish();
  updateSpilledFileStats(currentFile_->size());
  finishedFiles_.push_back(SpillFileInfo{
      .id = currentFile_->id(),
      .type = type_,
      .path = currentFile_->path(),
      .size = currentFile_->size(),
      .numSortKeys = numSortKeys_,
      .sortFlags = sortCompareFlags_,
      .compressionKind = compressionKind_});
  currentFile_.reset();
}

size_t SpillWriter::numFinishedFiles() const {
  return finishedFiles_.size();
}

uint64_t SpillWriter::flush() {
  if (batch_ == nullptr) {
    return 0;
  }

  auto* file = ensureFile();
  VELOX_CHECK_NOT_NULL(file);

  IOBufOutputStream out(
      *pool_, nullptr, std::max<int64_t>(64 * 1024, batch_->size()));
  uint64_t flushTimeUs{0};
  {
    MicrosecondTimer timer(&flushTimeUs);
    batch_->flush(&out);
  }
  batch_.reset();

  uint64_t writeTimeUs{0};
  uint64_t writtenBytes{0};
  auto iobuf = out.getIOBuf();
  {
    MicrosecondTimer timer(&writeTimeUs);
    writtenBytes = file->write(std::move(iobuf));
  }
  updateWriteStats(writtenBytes, flushTimeUs, writeTimeUs);
  updateAndCheckSpillLimitCb_(writtenBytes);
  return writtenBytes;
}

uint64_t SpillWriter::write(
    const RowVectorPtr& rows,
    const folly::Range<IndexRange*>& indices) {
  checkNotFinished();

  uint64_t timeUs{0};
  {
    MicrosecondTimer timer(&timeUs);
    if (batch_ == nullptr) {
      serializer::presto::PrestoVectorSerde::PrestoOptions options = {
          kDefaultUseLosslessTimestamp, compressionKind_, true /*nullsFirst*/};
      batch_ = std::make_unique<VectorStreamGroup>(pool_);
      batch_->createStreamTree(
          std::static_pointer_cast<const RowType>(rows->type()),
          1'000,
          &options);
    }
    batch_->append(rows, indices);
  }
  updateAppendStats(rows->size(), timeUs);
  if (batch_->size() < writeBufferSize_) {
    return 0;
  }
  return flush();
}

void SpillWriter::updateAppendStats(
    uint64_t numRows,
    uint64_t serializationTimeUs) {
  auto statsLocked = stats_->wlock();
  statsLocked->spilledRows += numRows;
  statsLocked->spillSerializationTimeUs += serializationTimeUs;
  common::updateGlobalSpillAppendStats(numRows, serializationTimeUs);
}

void SpillWriter::updateWriteStats(
    uint64_t spilledBytes,
    uint64_t flushTimeUs,
    uint64_t fileWriteTimeUs) {
  auto statsLocked = stats_->wlock();
  statsLocked->spilledBytes += spilledBytes;
  statsLocked->spillFlushTimeUs += flushTimeUs;
  statsLocked->spillWriteTimeUs += fileWriteTimeUs;
  ++statsLocked->spillWrites;
  common::updateGlobalSpillWriteStats(
      spilledBytes, flushTimeUs, fileWriteTimeUs);
}

void SpillWriter::updateSpilledFileStats(uint64_t fileSize) {
  ++stats_->wlock()->spilledFiles;
  addThreadLocalRuntimeStat(
      "spillFileSize", RuntimeCounter(fileSize, RuntimeCounter::Unit::kBytes));
  common::incrementGlobalSpilledFiles();
}

void SpillWriter::finishFile() {
  checkNotFinished();
  flush();
  closeFile();
  VELOX_CHECK_NULL(currentFile_);
}

SpillFiles SpillWriter::finish() {
  checkNotFinished();
  auto finishGuard = folly::makeGuard([this]() { finished_ = true; });

  finishFile();
  return std::move(finishedFiles_);
}

std::vector<std::string> SpillWriter::testingSpilledFilePaths() const {
  checkNotFinished();

  std::vector<std::string> spilledFilePaths;
  for (auto& file : finishedFiles_) {
    spilledFilePaths.push_back(file.path);
  }
  if (currentFile_ != nullptr) {
    spilledFilePaths.push_back(currentFile_->path());
  }
  return spilledFilePaths;
}

std::vector<uint32_t> SpillWriter::testingSpilledFileIds() const {
  checkNotFinished();

  std::vector<uint32_t> fileIds;
  for (auto& file : finishedFiles_) {
    fileIds.push_back(file.id);
  }
  if (currentFile_ != nullptr) {
    fileIds.push_back(currentFile_->id());
  }
  return fileIds;
}

std::unique_ptr<SpillReadFile> SpillReadFile::create(
    const SpillFileInfo& fileInfo,
    uint64_t bufferSize,
    memory::MemoryPool* pool,
    folly::Synchronized<common::SpillStats>* stats) {
  return std::unique_ptr<SpillReadFile>(new SpillReadFile(
      fileInfo.id,
      fileInfo.path,
      fileInfo.size,
      bufferSize,
      fileInfo.type,
      fileInfo.numSortKeys,
      fileInfo.sortFlags,
      fileInfo.compressionKind,
      pool,
      stats));
}

SpillReadFile::SpillReadFile(
    uint32_t id,
    const std::string& path,
    uint64_t size,
    uint64_t bufferSize,
    const RowTypePtr& type,
    uint32_t numSortKeys,
    const std::vector<CompareFlags>& sortCompareFlags,
    common::CompressionKind compressionKind,
    memory::MemoryPool* pool,
    folly::Synchronized<common::SpillStats>* stats)
    : id_(id),
      path_(path),
      size_(size),
      type_(type),
      numSortKeys_(numSortKeys),
      sortCompareFlags_(sortCompareFlags),
      compressionKind_(compressionKind),
      readOptions_{
          kDefaultUseLosslessTimestamp,
          compressionKind_,
          /*nullsFirst=*/true},
      pool_(pool),
      stats_(stats) {
  auto fs = filesystems::getFileSystem(path_, nullptr);
  auto file = fs->openFileForRead(path_);
  input_ = std::make_unique<SpillInputStream>(
      std::move(file), bufferSize, pool_, stats_);
}

bool SpillReadFile::nextBatch(RowVectorPtr& rowVector) {
  if (input_->atEnd()) {
    return false;
  }

  uint64_t timeUs{0};
  {
    MicrosecondTimer timer{&timeUs};
    VectorStreamGroup::read(
        input_.get(), pool_, type_, &rowVector, &readOptions_);
  }
  stats_->wlock()->spillDeserializationTimeUs += timeUs;
  common::updateGlobalSpillDeserializationTimeUs(timeUs);

  return true;
}
} // namespace facebook::velox::exec
