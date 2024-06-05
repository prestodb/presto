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

#include "velox/dwio/common/SeekableInputStream.h"

#include <algorithm>
#include <iomanip>

namespace facebook::velox::dwio::common {

void printBuffer(std::ostream& out, const char* buffer, uint64_t length) {
  const uint64_t width = 24;
  out << std::hex;
  for (uint64_t line = 0; line < (length + width - 1) / width; ++line) {
    out << std::setfill('0') << std::setw(7) << (line * width);
    for (uint64_t byte = 0; byte < width && line * width + byte < length;
         ++byte) {
      out << " " << std::setfill('0') << std::setw(2)
          << static_cast<uint64_t>(0xff & buffer[line * width + byte]);
    }
    out << "\n";
  }
  out << std::dec;
}

uint64_t PositionProvider::next() {
  uint64_t result = *position_;
  ++position_;
  return result;
}

bool PositionProvider::hasNext() const {
  return position_ != end_;
}

void SeekableInputStream::readFully(char* buffer, size_t bufferSize) {
  size_t pos = 0;
  size_t readLength = 0;
  size_t bytesToCopy = 0;
  while (pos < bufferSize) {
    const void* chunk;
    int32_t length;
    VELOX_CHECK(Next(&chunk, &length), "bad read in readFully");
    readLength = static_cast<size_t>(length);
    bytesToCopy = std::min(readLength, bufferSize - pos);
    auto* bytes = reinterpret_cast<const char*>(chunk);
    std::copy(bytes, bytes + bytesToCopy, buffer + pos);
    pos += bytesToCopy;
  }
  // return remaining bytes back to stream
  if (bytesToCopy < readLength) {
    BackUp(readLength - bytesToCopy);
  }
}

SeekableArrayInputStream::SeekableArrayInputStream(
    const unsigned char* values,
    uint64_t size,
    uint64_t blkSize)
    : data_(reinterpret_cast<const char*>(values)), dataRead_{nullptr} {
  length_ = size;
  position_ = 0;
  blockSize_ = blkSize == 0 ? length_ : blkSize;
}

SeekableArrayInputStream::SeekableArrayInputStream(
    const char* values,
    uint64_t size,
    uint64_t blkSize)
    : data_(values), dataRead_{nullptr} {
  length_ = size;
  position_ = 0;
  blockSize_ = blkSize == 0 ? length_ : blkSize;
}

SeekableArrayInputStream::SeekableArrayInputStream(
    std::unique_ptr<char[]> values,
    uint64_t size,
    uint64_t blkSize)
    : ownedData_(std::move(values)),
      data_(ownedData_.get()),
      dataRead_{nullptr} {
  length_ = size;
  position_ = 0;
  blockSize_ = blkSize == 0 ? length_ : blkSize;
}

SeekableArrayInputStream::SeekableArrayInputStream(
    std::function<std::tuple<const char*, uint64_t>()> read,
    uint64_t blkSize)
    : data_(nullptr), dataRead_{std::move(read)} {
  position_ = 0;
  length_ = 0;
  blockSize_ = blkSize;
}

void SeekableArrayInputStream::loadIfAvailable() {
  if (FOLLY_LIKELY(dataRead_ == nullptr)) {
    return;
  }
  const auto result = dataRead_();
  auto size = std::get<1>(result);
  VELOX_CHECK_LT(size, MAX_UINT64, "invalid data size");
  data_ = std::get<0>(result);
  length_ = size;
  if (blockSize_ == 0) {
    blockSize_ = length_;
  }
  // just load once
  dataRead_ = nullptr;
}

bool SeekableArrayInputStream::Next(const void** buffer, int32_t* size) {
  loadIfAvailable();
  const uint64_t currentSize = std::min(length_ - position_, blockSize_);
  if (currentSize > 0) {
    *buffer = data_ + position_;
    *size = static_cast<int32_t>(currentSize);
    position_ += currentSize;
    totalRead_ += currentSize;
    return true;
  }

  *size = 0;
  return false;
}

void SeekableArrayInputStream::BackUp(int32_t count) {
  loadIfAvailable();

  if (count >= 0) {
    const uint64_t unsignedCount = static_cast<uint64_t>(count);
    VELOX_CHECK_LE(unsignedCount, blockSize_, "Can't backup that much!");
    VELOX_CHECK_LE(unsignedCount, position_, "Can't backup that much!");
    position_ -= unsignedCount;
  }
}

bool SeekableArrayInputStream::SkipInt64(int64_t count) {
  loadIfAvailable();

  if (count >= 0) {
    const uint64_t unsignedCount = static_cast<uint64_t>(count);
    if (unsignedCount + position_ <= length_) {
      position_ += unsignedCount;
      return true;
    }
    position_ = length_;
  }
  return false;
}

google::protobuf::int64 SeekableArrayInputStream::ByteCount() const {
  return static_cast<google::protobuf::int64>(position_);
}

void SeekableArrayInputStream::seekToPosition(PositionProvider& position) {
  position_ = position.next();
}

std::string SeekableArrayInputStream::getName() const {
  return folly::to<std::string>(
      "SeekableArrayInputStream ", position_, " of ", length_);
}

size_t SeekableArrayInputStream::positionSize() {
  // not compressed, so only need 1 position (uncompressed position)
  return 1;
}

static uint64_t computeBlock(uint64_t request, uint64_t length) {
  return std::min(length, request == 0 ? 256 * 1024 : request);
}

SeekableFileInputStream::SeekableFileInputStream(
    std::shared_ptr<ReadFileInputStream> input,
    uint64_t offset,
    uint64_t byteCount,
    memory::MemoryPool& pool,
    LogType logType,
    uint64_t blockSize)
    : input_(std::move(input)),
      logType_(logType),
      start_(offset),
      length_(byteCount),
      blockSize_(computeBlock(blockSize, length_)),
      pool_(&pool),
      buffer_{pool} {
  position_ = 0;
  pushback_ = 0;
}

bool SeekableFileInputStream::Next(const void** data, int32_t* size) {
  uint64_t bytesRead;
  if (pushback_ != 0) {
    *data = buffer_.data() + (buffer_.size() - pushback_);
    bytesRead = pushback_;
  } else {
    bytesRead = std::min(length_ - position_, blockSize_);
    buffer_.resize(bytesRead);
    if (bytesRead > 0) {
      input_->read(buffer_.data(), bytesRead, start_ + position_, logType_);
      *data = static_cast<void*>(buffer_.data());
    }
  }
  position_ += bytesRead;
  pushback_ = 0;
  *size = static_cast<int32_t>(bytesRead);
  return bytesRead != 0;
}

void SeekableFileInputStream::BackUp(int32_t signedCount) {
  VELOX_CHECK_GE(signedCount, 0, "can't backup negative distances");
  VELOX_CHECK_EQ(pushback_, 0, "can't backup unless we just called Next");
  const uint64_t count = static_cast<uint64_t>(signedCount);
  VELOX_CHECK_LE(count, blockSize_, "can't backup that far");
  VELOX_CHECK_LE(count, position_, "can't backup that far");
  pushback_ = static_cast<uint64_t>(count);
  position_ -= pushback_;
}

bool SeekableFileInputStream::SkipInt64(int64_t signedCount) {
  if (signedCount < 0) {
    return false;
  }
  const uint64_t count = static_cast<uint64_t>(signedCount);
  position_ = std::min(position_ + count, length_);
  pushback_ = 0;
  return position_ < length_;
}

google::protobuf::int64 SeekableFileInputStream::ByteCount() const {
  return static_cast<google::protobuf::int64>(position_);
}

void SeekableFileInputStream::seekToPosition(PositionProvider& location) {
  position_ = location.next();
  VELOX_CHECK_LE(position_, length_, "seek too far");
  pushback_ = 0;
}

std::string SeekableFileInputStream::getName() const {
  return folly::to<std::string>(
      input_->getName(), " from ", start_, " for ", length_);
}

size_t SeekableFileInputStream::positionSize() {
  // not compressed, so only need 1 position (uncompressed position)
  return 1;
}

} // namespace facebook::velox::dwio::common
