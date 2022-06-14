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

#include "velox/dwio/dwrf/common/PagedInputStream.h"
#include "velox/dwio/common/SeekableInputStream.h"
#include "velox/dwio/common/exception/Exception.h"

namespace facebook::velox::dwrf {

void PagedInputStream::prepareOutputBuffer(uint64_t uncompressedLength) {
  if (!outputBuffer_ || uncompressedLength > outputBuffer_->capacity()) {
    outputBuffer_ = std::make_unique<dwio::common::DataBuffer<char>>(
        pool_, uncompressedLength);
  }
}

void PagedInputStream::readBuffer(bool failOnEof) {
  int32_t length;
  if (!input_->Next(
          reinterpret_cast<const void**>(&inputBufferPtr_), &length)) {
    DWIO_ENSURE(!failOnEof, getName(), ", read past EOF");
    state_ = State::END;
    inputBufferStart_ = nullptr;
    inputBufferPtr_ = nullptr;
    inputBufferPtrEnd_ = nullptr;
  } else {
    inputBufferStart_ = inputBufferPtr_;
    inputBufferPtrEnd_ = inputBufferPtr_ + length;
  }
}

uint32_t PagedInputStream::readByte(bool failOnEof) {
  if (UNLIKELY(inputBufferPtr_ == inputBufferPtrEnd_)) {
    readBuffer(failOnEof);
    if (state_ == State::END) {
      return 0;
    }
  }
  return static_cast<unsigned char>(*(inputBufferPtr_++));
}

void PagedInputStream::readHeader() {
  uint32_t header = readByte(false);

  lastHeaderOffset_ =
      input_->ByteCount() - (inputBufferPtrEnd_ - inputBufferPtr_) - 1;
  bytesReturnedAtLastHeaderOffset_ = bytesReturned_;

  if (state_ != State::END) {
    header |= readByte(true) << 8;
    header |= readByte(true) << 16;
    if (header & 1) {
      state_ = State::ORIGINAL;
    } else {
      state_ = State::START;
    }
    remainingLength_ = header >> 1;
  } else {
    remainingLength_ = 0;
  }
}

const char* PagedInputStream::ensureInput(size_t availableInputBytes) {
  auto input = inputBufferPtr_;
  if (remainingLength_ <= availableInputBytes) {
    inputBufferPtr_ += availableInputBytes;
    return input;
  }
  // make sure input buffer has capacity
  if (inputBuffer_.capacity() < remainingLength_) {
    inputBuffer_.reserve(remainingLength_);
  }

  std::copy(
      inputBufferPtr_,
      inputBufferPtr_ + availableInputBytes,
      inputBuffer_.data());
  inputBufferPtr_ += availableInputBytes;

  for (size_t pos = availableInputBytes; pos < remainingLength_;) {
    readBuffer(true);
    availableInputBytes = std::min(
        static_cast<size_t>(inputBufferPtrEnd_ - inputBufferPtr_),
        remainingLength_ - pos);
    std::copy(
        inputBufferPtr_,
        inputBufferPtr_ + availableInputBytes,
        inputBuffer_.data() + pos);
    pos += availableInputBytes;
    inputBufferPtr_ += availableInputBytes;
  }
  return inputBuffer_.data();
}

bool PagedInputStream::Next(const void** data, int32_t* size) {
  // if the user pushed back, return them the partial buffer
  if (outputBufferLength_) {
    *data = outputBufferPtr_;
    *size = static_cast<int32_t>(outputBufferLength_);
    outputBufferPtr_ += outputBufferLength_;
    bytesReturned_ += outputBufferLength_;
    outputBufferLength_ = 0;
    return true;
  }

  // release previous decryption buffer
  decryptionBuffer_ = nullptr;

  if (state_ == State::HEADER || remainingLength_ == 0) {
    readHeader();
  }
  if (state_ == State::END) {
    return false;
  }
  if (inputBufferPtr_ == inputBufferPtrEnd_) {
    readBuffer(true);
  }

  size_t availSize = std::min(
      static_cast<size_t>(inputBufferPtrEnd_ - inputBufferPtr_),
      remainingLength_);
  // in the case when decompression or decryption is needed, need to copy data
  // to input buffer if the input doesn't contain the entire block
  bool original = !decrypter_ && (state_ == State::ORIGINAL);
  const char* input = nullptr;
  // if no decompression or decryption is needed, simply adjust the output
  // pointer. Otherwise, make sure we have continuous block
  if (original) {
    *data = inputBufferPtr_;
    *size = static_cast<int32_t>(availSize);
    outputBufferPtr_ = inputBufferPtr_ + availSize;
    inputBufferPtr_ += availSize;
    remainingLength_ -= availSize;
  } else {
    input = ensureInput(availSize);
  }

  // perform decryption
  if (decrypter_) {
    decryptionBuffer_ =
        decrypter_->decrypt(folly::StringPiece{input, remainingLength_});
    input = reinterpret_cast<const char*>(decryptionBuffer_->data());
    remainingLength_ = decryptionBuffer_->length();
    *data = input;
    *size = remainingLength_;
    outputBufferPtr_ = input + remainingLength_;
  }

  // perform decompression
  if (state_ == State::START) {
    DWIO_ENSURE_NOT_NULL(decompressor_.get(), "invalid stream state");
    prepareOutputBuffer(
        decompressor_->getUncompressedLength(input, remainingLength_));
    outputBufferLength_ = decompressor_->decompress(
        input,
        remainingLength_,
        outputBuffer_->data(),
        outputBuffer_->capacity());
    *data = outputBuffer_->data();
    *size = static_cast<int32_t>(outputBufferLength_);
    outputBufferPtr_ = outputBuffer_->data() + outputBufferLength_;
    // release decryption buffer
    decryptionBuffer_ = nullptr;
  }

  if (!original) {
    remainingLength_ = 0;
    state_ = State::HEADER;
  }

  outputBufferLength_ = 0;
  bytesReturned_ += *size;
  return true;
}

void PagedInputStream::BackUp(int32_t count) {
  DWIO_ENSURE(
      outputBufferPtr_ != nullptr,
      "Backup without previous Next in ",
      getName());
  if (state_ == State::ORIGINAL) {
    VELOX_CHECK(
        outputBufferPtr_ >= inputBufferStart_ &&
        outputBufferPtr_ <= inputBufferPtrEnd_);
    // 'outputBufferPtr_' ranges over the input buffer if there is no
    // decompression / decryption. Check that we do not back out of
    // the last range returned from input_->Next().
    VELOX_CHECK_GE(
        inputBufferPtr_ - static_cast<size_t>(count), inputBufferStart_);
  }
  outputBufferPtr_ -= static_cast<size_t>(count);
  outputBufferLength_ += static_cast<size_t>(count);
  bytesReturned_ -= count;
}

bool PagedInputStream::Skip(int32_t count) {
  // this is a stupid implementation for now.
  // should skip entire blocks without decompressing
  while (count > 0) {
    const void* ptr;
    int32_t len;
    if (!Next(&ptr, &len)) {
      return false;
    }
    if (len > count) {
      BackUp(len - count);
      count = 0;
    } else {
      count -= len;
    }
  }
  return true;
}

void PagedInputStream::clearDecompressionState() {
  state_ = State::HEADER;
  outputBufferLength_ = 0;
  remainingLength_ = 0;
  inputBufferPtr_ = nullptr;
  inputBufferPtrEnd_ = nullptr;
}

void PagedInputStream::seekToPosition(
    dwio::common::PositionProvider& positionProvider) {
  auto compressedOffset = positionProvider.next();
  auto uncompressedOffset = positionProvider.next();

  // If we are directly returning views into input, we can only backup
  // to the beginning of the last view. If we are returning views into
  // uncompressed data, we can backup to the beginning of the
  // decompressed buffer
  auto alreadyRead = bytesReturned_ - bytesReturnedAtLastHeaderOffset_;

  // outsideOriginalWindow is true if we are returning views into
  // the input stream's buffer and we are seeking below the start of the last
  // window.
  auto outsideOriginalWindow = [&]() {
    return state_ == State::ORIGINAL && compressedOffset == lastHeaderOffset_ &&
        uncompressedOffset < alreadyRead &&
        inputBufferPtrEnd_ - inputBufferStart_ <
        alreadyRead - uncompressedOffset;
  };

  if (compressedOffset != lastHeaderOffset_ || outsideOriginalWindow()) {
    std::vector<uint64_t> positions = {compressedOffset};
    auto provider = dwio::common::PositionProvider(positions);
    input_->seekToPosition(provider);

    clearDecompressionState();

    Skip(uncompressedOffset);
  } else {
    if (uncompressedOffset < alreadyRead) {
      BackUp(alreadyRead - uncompressedOffset);
    } else {
      Skip(uncompressedOffset - alreadyRead);
    }
  }
}

} // namespace facebook::velox::dwrf
