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

#pragma once

#include "velox/dwio/common/SeekableInputStream.h"
#include "velox/dwio/dwrf/common/Compression.h"

namespace facebook::velox::dwrf {

class PagedInputStream : public dwio::common::SeekableInputStream {
 public:
  PagedInputStream(
      std::unique_ptr<SeekableInputStream> inStream,
      memory::MemoryPool& memPool,
      std::unique_ptr<Decompressor> decompressor,
      const dwio::common::encryption::Decrypter* decrypter,
      const std::string& streamDebugInfo)
      : input_(std::move(inStream)),
        pool_(memPool),
        inputBuffer_(pool_),
        decompressor_{std::move(decompressor)},
        decrypter_{decrypter},
        streamDebugInfo_{streamDebugInfo} {
    DWIO_ENSURE(
        decompressor_ || decrypter_,
        "one of decompressor or decryptor is required");
  }

  bool Next(const void** data, int32_t* size) override;
  void BackUp(int32_t count) override;
  bool Skip(int32_t count) override;
  google::protobuf::int64 ByteCount() const override {
    return bytesReturned_;
  }
  void seekToPosition(dwio::common::PositionProvider& position) override;
  std::string getName() const override {
    return folly::to<std::string>(
        "PagedInputStream StreamInfo (",
        streamDebugInfo_,
        ") input stream (",
        input_->getName(),
        ") State (",
        state_,
        ") remaining length (",
        remainingLength_,
        ")");
  }

  size_t positionSize() override {
    // not compressed, so need 2 positions (compressed position + uncompressed
    // position)
    return 2;
  }

 protected:
  // Special constructor used by ZlibDecompressionStream
  PagedInputStream(
      std::unique_ptr<SeekableInputStream> inStream,
      memory::MemoryPool& memPool,
      const std::string& streamDebugInfo)
      : input_(std::move(inStream)),
        pool_(memPool),
        inputBuffer_(pool_),
        decompressor_{nullptr},
        decrypter_{nullptr},
        streamDebugInfo_{streamDebugInfo} {}

  void prepareOutputBuffer(uint64_t uncompressedLength);

  void readBuffer(bool failOnEof);

  uint32_t readByte(bool failOnEof);

  void readHeader();

  void clearDecompressionState();

  enum class State { HEADER, START, ORIGINAL, END };

  // make sure input is contiguous for decompression/decryption
  const char* ensureInput(size_t availableInputBytes);

  // input stream where to read compressed/encrypted data
  std::unique_ptr<SeekableInputStream> input_;
  memory::MemoryPool& pool_;

  // Offset in input_ of the last header read
  uint64_t lastHeaderOffset_{0};

  // The value of bytesReturned_ at the time when last header was read
  uint64_t bytesReturnedAtLastHeaderOffset_{0};

  // buffer to hold an entire compressed/encrypted block allowing
  // decompression/decryption algorithm to work on contiguous block
  dwio::common::DataBuffer<char> inputBuffer_;

  // uncompressed output
  std::unique_ptr<dwio::common::DataBuffer<char>> outputBuffer_{nullptr};

  // unencrypted output
  std::unique_ptr<folly::IOBuf> decryptionBuffer_{nullptr};

  // the current state
  State state_{State::HEADER};

  // the start of the current output buffer
  const char* outputBufferPtr_{nullptr};

  // the size of the current output buffer
  size_t outputBufferLength_{0};

  // the size of the current chunk (in its compressed/encrypted form)
  size_t remainingLength_{0};

  // The first byte in the range from last call to 'input_->Next()'.
  const char* inputBufferStart_{nullptr};

  // The first byte to return in Next. Not the same as inputBufferStart_ if
  // there has been a BackUp().
  const char* inputBufferPtr_{nullptr};

  // The first byte after the last range returned by 'input_->Next()'.
  const char* inputBufferPtrEnd_{nullptr};

  // bytes returned by this stream
  uint64_t bytesReturned_{0};

  // Size returned by the previous call to Next().
  int32_t lastWindowSize_{0};

  // decompressor
  std::unique_ptr<Decompressor> decompressor_;

  // decrypter
  const dwio::common::encryption::Decrypter* decrypter_;

 private:
  // Stream Debug Info
  const std::string streamDebugInfo_;
};

} // namespace facebook::velox::dwrf
