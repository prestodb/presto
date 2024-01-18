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

#include "velox/common/memory/ByteStream.h"

namespace facebook::velox {

std::string ByteRange::toString() const {
  return fmt::format("[{} starting at {}]", succinctBytes(size), position);
}

std::string ByteInputStream::toString() const {
  std::stringstream oss;
  oss << ranges_.size() << " ranges (position/size) [";
  for (const auto& range : ranges_) {
    oss << "(" << range.position << "/" << range.size
        << (&range == current_ ? " current" : "") << ")";
    if (&range != &ranges_.back()) {
      oss << ",";
    }
  }
  oss << "]";
  return oss.str();
}

bool ByteInputStream::atEnd() const {
  if (!current_) {
    return false;
  }
  if (current_->position < current_->size) {
    return false;
  }

  VELOX_CHECK(current_ >= ranges_.data() && current_ <= &ranges_.back());
  return current_ == &ranges_.back();
}

size_t ByteInputStream::size() const {
  size_t total = 0;
  for (const auto& range : ranges_) {
    total += range.size;
  }
  return total;
}

size_t ByteInputStream::remainingSize() const {
  if (ranges_.empty()) {
    return 0;
  }
  const auto* lastRange = &ranges_[ranges_.size() - 1];
  auto cur = current_;
  size_t total = cur->size - cur->position;
  while (++cur <= lastRange) {
    total += cur->size;
  }
  return total;
}

std::streampos ByteInputStream::tellp() const {
  if (ranges_.empty()) {
    return 0;
  }
  assert(current_);
  int64_t size = 0;
  for (auto& range : ranges_) {
    if (&range == current_) {
      return current_->position + size;
    }
    size += range.size;
  }
  VELOX_FAIL("ByteInputStream 'current_' is not in 'ranges_'.");
}

void ByteInputStream::seekp(std::streampos position) {
  if (ranges_.empty() && position == 0) {
    return;
  }
  int64_t toSkip = position;
  for (auto& range : ranges_) {
    if (toSkip <= range.size) {
      current_ = &range;
      current_->position = toSkip;
      return;
    }
    toSkip -= range.size;
  }
  VELOX_FAIL("Seeking past end of ByteInputStream: {}", position);
}

void ByteInputStream::next(bool throwIfPastEnd) {
  VELOX_CHECK(current_ >= &ranges_[0]);
  size_t position = current_ - &ranges_[0];
  VELOX_CHECK_LT(position, ranges_.size());
  if (position == ranges_.size() - 1) {
    if (throwIfPastEnd) {
      VELOX_FAIL("Reading past end of ByteInputStream");
    }
    return;
  }
  ++current_;
  current_->position = 0;
}

uint8_t ByteInputStream::readByte() {
  if (current_->position < current_->size) {
    return current_->buffer[current_->position++];
  }
  next();
  return readByte();
}

void ByteInputStream::readBytes(uint8_t* bytes, int32_t size) {
  int32_t offset = 0;
  for (;;) {
    int32_t available = current_->size - current_->position;
    int32_t numUsed = std::min(available, size);
    simd::memcpy(
        bytes + offset, current_->buffer + current_->position, numUsed);
    offset += numUsed;
    size -= numUsed;
    current_->position += numUsed;
    if (!size) {
      return;
    }
    next();
  }
}

std::string_view ByteInputStream::nextView(int32_t size) {
  if (current_->position == current_->size) {
    if (current_ == &ranges_.back()) {
      return std::string_view(nullptr, 0);
    }
    next();
  }
  VELOX_CHECK(current_->size);
  auto position = current_->position;
  auto viewSize = std::min(current_->size - current_->position, size);
  current_->position += viewSize;
  return std::string_view(
      reinterpret_cast<char*>(current_->buffer) + position, viewSize);
}

void ByteInputStream::skip(int32_t size) {
  for (;;) {
    int32_t available = current_->size - current_->position;
    int32_t numUsed = std::min(available, size);
    size -= numUsed;
    current_->position += numUsed;
    if (!size) {
      return;
    }
    next();
  }
}

size_t ByteOutputStream::size() const {
  if (ranges_.empty()) {
    return 0;
  }
  size_t total = 0;
  for (auto i = 0; i < ranges_.size() - 1; ++i) {
    total += ranges_[i].size;
  }
  return total + std::max(ranges_.back().position, lastRangeEnd_);
}

void ByteOutputStream::appendBool(bool value, int32_t count) {
  VELOX_DCHECK(isBits_);

  if (count == 1 && current_->size > current_->position) {
    bits::setBit(
        reinterpret_cast<uint64_t*>(current_->buffer),
        current_->position,
        value);
    ++current_->position;
    return;
  }

  int32_t offset{0};
  for (;;) {
    const int32_t bitsFit =
        std::min(count - offset, current_->size - current_->position);
    bits::fillBits(
        reinterpret_cast<uint64_t*>(current_->buffer),
        current_->position,
        current_->position + bitsFit,
        value);
    current_->position += bitsFit;
    offset += bitsFit;
    if (offset == count) {
      return;
    }
    extend(bits::nbytes(count - offset));
  }
}

void ByteOutputStream::appendBits(
    const uint64_t* bits,
    int32_t begin,
    int32_t end) {
  VELOX_DCHECK(isBits_);

  const int32_t count = end - begin;
  int32_t offset = 0;
  for (;;) {
    const int32_t bitsFit =
        std::min(count - offset, current_->size - current_->position);
    bits::copyBits(
        bits,
        begin + offset,
        reinterpret_cast<uint64_t*>(current_->buffer),
        current_->position,
        bitsFit);

    current_->position += bitsFit;
    offset += bitsFit;
    if (offset == count) {
      return;
    }
    extend(bits::nbytes(count - offset));
  }
}

void ByteOutputStream::appendStringView(StringView value) {
  appendStringView((std::string_view)value);
}

void ByteOutputStream::appendStringView(std::string_view value) {
  const int32_t bytes = value.size();
  int32_t offset = 0;
  for (;;) {
    const int32_t bytesFit =
        std::min(bytes - offset, current_->size - current_->position);
    simd::memcpy(
        current_->buffer + current_->position, value.data() + offset, bytesFit);
    current_->position += bytesFit;
    offset += bytesFit;
    if (offset == bytes) {
      return;
    }
    extend(bytes - offset);
  }
}

std::streampos ByteOutputStream::tellp() const {
  if (ranges_.empty()) {
    return 0;
  }
  assert(current_);
  int64_t size = 0;
  for (auto& range : ranges_) {
    if (&range == current_) {
      return current_->position + size;
    }
    size += range.size;
  }
  VELOX_FAIL("ByteOutputStream 'current_' is not in 'ranges_'.");
}

void ByteOutputStream::seekp(std::streampos position) {
  int64_t toSkip = position;
  // Record how much was written pre-seek.
  updateEnd();
  if (ranges_.empty() && position == 0) {
    return;
  }
  for (auto& range : ranges_) {
    if (toSkip <= range.size) {
      current_ = &range;
      current_->position = toSkip;
      return;
    }
    toSkip -= range.size;
  }
  VELOX_FAIL("Seeking past end of ByteOutputStream: {}", position);
}

void ByteOutputStream::flush(OutputStream* out) {
  updateEnd();
  for (int32_t i = 0; i < ranges_.size(); ++i) {
    int32_t count = i == ranges_.size() - 1 ? lastRangeEnd_ : ranges_[i].size;
    int32_t bytes = isBits_ ? bits::nbytes(count) : count;
    if (isBits_ && isReverseBitOrder_ && !isReversed_) {
      bits::reverseBits(ranges_[i].buffer, bytes);
    }
    out->write(reinterpret_cast<char*>(ranges_[i].buffer), bytes);
  }
  if (isBits_ && isReverseBitOrder_) {
    isReversed_ = true;
  }
}

char* ByteOutputStream::writePosition() {
  if (ranges_.empty()) {
    return nullptr;
  }
  return reinterpret_cast<char*>(current_->buffer) + current_->position;
}

void ByteOutputStream::extend(int32_t bytes) {
  if (current_ && current_->position != current_->size) {
    LOG(FATAL) << "Extend ByteOutputStream before range full: "
               << current_->position << " vs. " << current_->size;
  }

  // Check if rewriting existing content. If so, move to next range and start at
  // 0.
  if ((current_ != nullptr) && (current_ != &ranges_.back())) {
    ++current_;
    current_->position = 0;
    return;
  }

  ranges_.emplace_back();
  current_ = &ranges_.back();
  lastRangeEnd_ = 0;
  arena_->newRange(
      newRangeSize(bytes),
      ranges_.size() == 1 ? nullptr : &ranges_[ranges_.size() - 2],
      current_);
  allocatedBytes_ += current_->size;
  VELOX_CHECK_GT(allocatedBytes_, 0);
  if (isBits_) {
    // size and position are in units of bits for a bits stream.
    current_->size *= 8;
  }
}

int32_t ByteOutputStream::newRangeSize(int32_t bytes) const {
  const int32_t newSize = allocatedBytes_ + bytes;
  if (newSize < 128) {
    return 128;
  }
  if (newSize < 512) {
    return bits::roundUp(bytes, 128);
  }
  if (newSize < memory::AllocationTraits::kPageSize) {
    return bits::roundUp(bytes, 512);
  }
  return bits::roundUp(bytes, memory::AllocationTraits::kPageSize);
}

void ByteOutputStream::ensureSpace(int32_t bytes) {
  const auto available = current_->size - current_->position;
  int64_t toExtend = bytes - available;
  const auto originalRangeIdx = current_ - ranges_.data();
  const auto originalPosition = current_->position;
  while (toExtend > 0) {
    current_->position = current_->size;
    extend(toExtend);
    toExtend -= current_->size;
  }
  // Restore original position.
  current_ = &ranges_[originalRangeIdx];
  current_->position = originalPosition;
}

ByteInputStream ByteOutputStream::inputStream() const {
  VELOX_CHECK(!ranges_.empty());
  updateEnd();
  auto rangeCopy = ranges_;
  rangeCopy.back().size = lastRangeEnd_;
  return ByteInputStream(std::move(rangeCopy));
}

std::string ByteOutputStream::toString() const {
  std::stringstream oss;
  oss << "ByteOutputStream[lastRangeEnd " << lastRangeEnd_ << ", "
      << ranges_.size() << " ranges (position/size) [";
  for (const auto& range : ranges_) {
    oss << "(" << range.position << "/" << range.size
        << (&range == current_ ? " current" : "") << ")";
    if (&range != &ranges_.back()) {
      oss << ",";
    }
  }
  oss << "]]";
  return oss.str();
}

namespace {
// The user data structure passed to folly iobuf for buffer ownership handling.
struct FreeData {
  std::shared_ptr<StreamArena> arena;
  std::function<void()> releaseFn;
};

FreeData* newFreeData(
    const std::shared_ptr<StreamArena>& arena,
    const std::function<void()>& releaseFn) {
  auto freeData = new FreeData();
  freeData->arena = arena;
  freeData->releaseFn = releaseFn;
  return freeData;
}

void freeFunc(void* /*data*/, void* userData) {
  auto* freeData = reinterpret_cast<FreeData*>(userData);
  freeData->arena.reset();
  if (freeData->releaseFn != nullptr) {
    freeData->releaseFn();
  }
  delete freeData;
}
} // namespace

std::unique_ptr<folly::IOBuf> IOBufOutputStream::getIOBuf(
    const std::function<void()>& releaseFn) {
  // Make an IOBuf for each range. The IOBufs keep shared ownership of
  // 'arena_'.
  std::unique_ptr<folly::IOBuf> iobuf;
  auto& ranges = out_->ranges();
  for (auto& range : ranges) {
    auto numValues =
        &range == &ranges.back() ? out_->lastRangeEnd() : range.size;
    auto userData = newFreeData(arena_, releaseFn);
    auto newBuf = folly::IOBuf::takeOwnership(
        reinterpret_cast<char*>(range.buffer), numValues, freeFunc, userData);
    if (iobuf) {
      iobuf->prev()->appendChain(std::move(newBuf));
    } else {
      iobuf = std::move(newBuf);
    }
  }
  return iobuf;
}

std::streampos IOBufOutputStream::tellp() const {
  return out_->tellp();
}

void IOBufOutputStream::seekp(std::streampos pos) {
  out_->seekp(pos);
}

} // namespace facebook::velox
