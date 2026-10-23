// Copyright (c) Facebook, Inc. and its affiliates.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "velox/dwio/common/compression/Compression.h"
#include "velox/dwio/common/compression/CompressionBufferPool.h"
#include "velox/dwio/common/exception/Exception.h"

#include <folly/logging/xlog.h>

namespace facebook::velox::dwio::common {

namespace {

// Returns true when the underlying stream has no more bytes.
// Distinguishes clean EOF from a short read (declared length past available).
bool fillFromInput(
    SeekableInputStream& input,
    const char*& start,
    const char*& end,
    bool failOnEof,
    int64_t remainingLength,
    int state,
    const std::string& streamName) {
  const void* buffer;
  int32_t size;
  if (!input.Next(&buffer, &size)) {
    // Bytes still expected from the declared stream length: this is a short
    // read (wrong footer length or partial cache page), not a clean EOF.
    if (remainingLength > 0) {
      DWIO_RAISE(
          "{}: short read, remaining length {}, state {}, "
          "declared compressed stream length exceeds available bytes",
          streamName,
          remainingLength,
          state);
    }
    DWIO_ENSURE(
        !failOnEof,
        "{}: unexpected EOF, remaining length {}, state {}",
        streamName,
        remainingLength,
        state);
    return false;
  }
  start = static_cast<const char*>(buffer);
  end = start + size;
  return true;
}

} // namespace

bool PagedInputStream::readBuffer(bool failOnEof) {
  DWIO_ENSURE_EQ(pendingBytes_, 0);

  while (true) {
    switch (state_) {
      case State::kStart:
      case State::kHeader: {
        // Need a full compression page header before reading data.
        while (inputBufferStart_ + kHeaderSize > inputBufferEnd_) {
          // Carry any partial header bytes already buffered.
          const auto buffered =
              static_cast<int32_t>(inputBufferEnd_ - inputBufferStart_);
          if (buffered > 0) {
            std::memcpy(
                headerBuffer_.data(), inputBufferStart_, buffered);
            headerBuffered_ = buffered;
          }
          if (!fillFromInput(
                  *input_,
                  inputBufferStart_,
                  inputBufferEnd_,
                  failOnEof,
                  remainingLength_,
                  static_cast<int>(state_),
                  getName())) {
            state_ = State::kEndOfInput;
            return false;
          }
          if (headerBuffered_ > 0) {
            const auto need = kHeaderSize - headerBuffered_;
            const auto available =
                static_cast<int32_t>(inputBufferEnd_ - inputBufferStart_);
            const auto copy = std::min(need, available);
            std::memcpy(
                headerBuffer_.data() + headerBuffered_,
                inputBufferStart_,
                copy);
            headerBuffered_ += copy;
            inputBufferStart_ += copy;
            if (headerBuffered_ < kHeaderSize) {
              continue;
            }
            // Header complete in headerBuffer_.
            parseHeader(headerBuffer_.data());
            headerBuffered_ = 0;
            state_ = State::kData;
            break;
          }
        }
        if (state_ != State::kData) {
          parseHeader(inputBufferStart_);
          inputBufferStart_ += kHeaderSize;
          state_ = State::kData;
        }
        break;
      }
      case State::kData: {
        // Copy/decompress up to remainingLength_ bytes of page payload.
        if (remainingLength_ == 0) {
          state_ = State::kStart;
          continue;
        }
        if (inputBufferStart_ == inputBufferEnd_) {
          if (!fillFromInput(
                  *input_,
                  inputBufferStart_,
                  inputBufferEnd_,
                  failOnEof,
                  remainingLength_,
                  static_cast<int>(state_),
                  getName())) {
            // fillFromInput raises on short read when remainingLength_ > 0.
            state_ = State::kEndOfInput;
            return false;
          }
        }
        const auto available =
            static_cast<int64_t>(inputBufferEnd_ - inputBufferStart_);
        const auto toConsume = std::min(available, remainingLength_);
        if (!decompressor_) {
          // Uncompressed page: expose input bytes directly.
          pendingOutput_ = inputBufferStart_;
          pendingBytes_ = static_cast<int32_t>(toConsume);
          inputBufferStart_ += toConsume;
          remainingLength_ -= toConsume;
          if (remainingLength_ == 0) {
            state_ = State::kStart;
          }
          return true;
        }
        // Accumulate compressed bytes then decompress one page.
        ensureCompressedCapacity(remainingLength_);
        const auto copy = std::min(available, remainingLength_);
        std::memcpy(
            compressedBuffer_.data() + compressedBuffered_,
            inputBufferStart_,
            copy);
        compressedBuffered_ += static_cast<int32_t>(copy);
        inputBufferStart_ += copy;
        remainingLength_ -= copy;
        if (remainingLength_ > 0) {
          // Need more compressed bytes for this page; pull next input chunk.
          continue;
        }
        // Full compressed page available; decompress into output buffer.
        const auto decompressedSize = decompressor_->decompress(
            compressedBuffer_.data(),
            compressedBuffered_,
            outputBuffer_.data(),
            outputBuffer_.size());
        compressedBuffered_ = 0;
        pendingOutput_ = outputBuffer_.data();
        pendingBytes_ = static_cast<int32_t>(decompressedSize);
        state_ = State::kStart;
        return true;
      }
      case State::kEndOfInput:
        return false;
    }
  }
}

} // namespace facebook::velox::dwio::common
