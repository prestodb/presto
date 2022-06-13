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
  size_t posn = 0;
  size_t readLength = 0;
  size_t bytesToCopy = 0;
  while (posn < bufferSize) {
    const void* chunk;
    int32_t length;
    DWIO_ENSURE(Next(&chunk, &length), "bad read in readFully");
    readLength = static_cast<size_t>(length);
    bytesToCopy = std::min(readLength, bufferSize - posn);
    auto bytes = reinterpret_cast<const char*>(chunk);
    std::copy(bytes, bytes + bytesToCopy, buffer + posn);
    posn += bytesToCopy;
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
    : data(reinterpret_cast<const char*>(values)), dataRead{nullptr} {
  length = size;
  position = 0;
  blockSize = blkSize == 0 ? length : blkSize;
}

SeekableArrayInputStream::SeekableArrayInputStream(
    const char* values,
    uint64_t size,
    uint64_t blkSize)
    : data(values), dataRead{nullptr} {
  length = size;
  position = 0;
  blockSize = blkSize == 0 ? length : blkSize;
}

SeekableArrayInputStream::SeekableArrayInputStream(
    std::unique_ptr<char[]> values,
    uint64_t size,
    uint64_t blkSize)
    : ownedData(std::move(values)), data(ownedData.get()), dataRead{nullptr} {
  length = size;
  position = 0;
  blockSize = blkSize == 0 ? length : blkSize;
}

SeekableArrayInputStream::SeekableArrayInputStream(
    std::function<std::tuple<const char*, uint64_t>()> read,
    uint64_t blkSize)
    : data(nullptr), dataRead{read} {
  position = 0;
  length = 0;
  blockSize = blkSize;
}

void SeekableArrayInputStream::loadIfAvailable() {
  if (UNLIKELY(!!dataRead)) {
    const auto result = dataRead();
    auto size = std::get<1>(result);
    DWIO_ENSURE_LT(size, MAX_UINT64, "invalid data size");
    data = std::get<0>(result);
    length = size;
    if (blockSize == 0) {
      blockSize = length;
    }

    // just load once
    dataRead = nullptr;
  }
}

bool SeekableArrayInputStream::Next(const void** buffer, int32_t* size) {
  loadIfAvailable();
  uint64_t currentSize = std::min(length - position, blockSize);
  if (currentSize > 0) {
    *buffer = data + position;
    *size = static_cast<int32_t>(currentSize);
    position += currentSize;
    return true;
  }

  *size = 0;
  return false;
}

void SeekableArrayInputStream::BackUp(int32_t count) {
  loadIfAvailable();

  if (count >= 0) {
    uint64_t unsignedCount = static_cast<uint64_t>(count);
    DWIO_ENSURE(
        unsignedCount <= blockSize && unsignedCount <= position,
        "Can't backup that much!");
    position -= unsignedCount;
  }
}

bool SeekableArrayInputStream::Skip(int32_t count) {
  loadIfAvailable();

  if (count >= 0) {
    uint64_t unsignedCount = static_cast<uint64_t>(count);
    if (unsignedCount + position <= length) {
      position += unsignedCount;
      return true;
    } else {
      position = length;
    }
  }
  return false;
}

google::protobuf::int64 SeekableArrayInputStream::ByteCount() const {
  return static_cast<google::protobuf::int64>(position);
}

void SeekableArrayInputStream::seekToPosition(PositionProvider& position) {
  this->position = position.next();
}

std::string SeekableArrayInputStream::getName() const {
  return folly::to<std::string>(
      "SeekableArrayInputStream ", position, " of ", length);
}

size_t SeekableArrayInputStream::positionSize() {
  // not compressed, so only need 1 position (uncompressed position)
  return 1;
}

static uint64_t computeBlock(uint64_t request, uint64_t length) {
  return std::min(length, request == 0 ? 256 * 1024 : request);
}

SeekableFileInputStream::SeekableFileInputStream(
    InputStream& stream,
    uint64_t offset,
    uint64_t byteCount,
    memory::MemoryPool& _pool,
    LogType logType,
    uint64_t _blockSize)
    : pool(_pool),
      input(stream),
      logType(logType),
      start(offset),
      length(byteCount),
      blockSize(computeBlock(_blockSize, length)),
      buffer{pool} {
  position = 0;
  pushBack = 0;
}

bool SeekableFileInputStream::Next(const void** data, int32_t* size) {
  uint64_t bytesRead;
  if (pushBack != 0) {
    *data = buffer.data() + (buffer.size() - pushBack);
    bytesRead = pushBack;
  } else {
    bytesRead = std::min(length - position, blockSize);
    buffer.resize(bytesRead);
    if (bytesRead > 0) {
      input.read(buffer.data(), bytesRead, start + position, logType);
      *data = static_cast<void*>(buffer.data());
    }
  }
  position += bytesRead;
  pushBack = 0;
  *size = static_cast<int32_t>(bytesRead);
  return bytesRead != 0;
}

void SeekableFileInputStream::BackUp(int32_t signedCount) {
  DWIO_ENSURE_GE(signedCount, 0, "can't backup negative distances");
  uint64_t count = static_cast<uint64_t>(signedCount);
  DWIO_ENSURE_EQ(pushBack, 0, "can't backup unless we just called Next");
  DWIO_ENSURE(count <= blockSize && count <= position, "can't backup that far");
  pushBack = static_cast<uint64_t>(count);
  position -= pushBack;
}

bool SeekableFileInputStream::Skip(int32_t signedCount) {
  if (signedCount < 0) {
    return false;
  }
  uint64_t count = static_cast<uint64_t>(signedCount);
  position = std::min(position + count, length);
  pushBack = 0;
  return position < length;
}

google::protobuf::int64 SeekableFileInputStream::ByteCount() const {
  return static_cast<google::protobuf::int64>(position);
}

void SeekableFileInputStream::seekToPosition(PositionProvider& location) {
  position = location.next();
  DWIO_ENSURE_LE(position, length, "seek too far");
  pushBack = 0;
}

std::string SeekableFileInputStream::getName() const {
  return folly::to<std::string>(
      input.getName(), " from ", start, " for ", length);
}

size_t SeekableFileInputStream::positionSize() {
  // not compressed, so only need 1 position (uncompressed position)
  return 1;
}

} // namespace facebook::velox::dwio::common
