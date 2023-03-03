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

#include <thrift/transport/TVirtualTransport.h>
#include "velox/dwio/common/BufferedInput.h"

namespace facebook::velox::parquet::thrift {

class ThriftTransport
    : public apache::thrift::transport::TVirtualTransport<ThriftTransport> {
 public:
  virtual uint32_t read(uint8_t* outputBuf, uint32_t len) = 0;
  virtual ~ThriftTransport() = default;
};

class ThriftStreamingTransport : public ThriftTransport {
 public:
  ThriftStreamingTransport(
      dwio::common::SeekableInputStream* inputStream,
      const char*& bufferStart,
      const char*& bufferEnd)
      : inputStream_(inputStream),
        bufferStart_(bufferStart),
        bufferEnd_(bufferEnd) {
    VELOX_CHECK_NOT_NULL(inputStream_);
    VELOX_CHECK_NOT_NULL(bufferStart_);
    VELOX_CHECK_NOT_NULL(bufferEnd_);
  }

  uint32_t read(uint8_t* outputBuf, uint32_t len) {
    uint32_t bytesToRead = len;
    while (bytesToRead > 0) {
      if (bufferEnd_ == bufferStart_) {
        int32_t size;
        if (!inputStream_->Next(
                reinterpret_cast<const void**>(&bufferStart_), &size)) {
          VELOX_FAIL("Reading past the end of the stream");
        }
        bufferEnd_ = bufferStart_ + size;
      }

      uint32_t bytesToReadInBuffer =
          std::min<uint32_t>(bufferEnd_ - bufferStart_, bytesToRead);
      memcpy(outputBuf, bufferStart_, bytesToReadInBuffer);
      bufferStart_ += bytesToReadInBuffer;
      bytesToRead -= bytesToReadInBuffer;
      outputBuf += bytesToReadInBuffer;
    }

    return len;
  }

 private:
  dwio::common::SeekableInputStream* inputStream_;
  const char*& bufferStart_;
  const char*& bufferEnd_;
};

class ThriftBufferedTransport : public ThriftTransport {
 public:
  ThriftBufferedTransport(const void* inputBuf, uint64_t len)
      : ThriftTransport(),
        inputBuf_(reinterpret_cast<const uint8_t*>(inputBuf)),
        size_(len),
        offset_(0) {}

  uint32_t read(uint8_t* outputBuf, uint32_t len) {
    DWIO_ENSURE(offset_ + len <= size_);
    memcpy(outputBuf, inputBuf_ + offset_, len);
    offset_ += len;
    return len;
  }

 private:
  const uint8_t* inputBuf_;
  const uint64_t size_;
  uint64_t offset_;
};

} // namespace facebook::velox::parquet::thrift
