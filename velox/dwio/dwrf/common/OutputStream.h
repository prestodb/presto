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

#include "velox/dwio/common/wrap/zero-copy-stream-wrapper.h"
#include "velox/dwio/dwrf/common/DataBufferHolder.h"
#include "velox/dwio/dwrf/common/wrap/dwrf-proto-wrapper.h"

namespace facebook::velox::dwrf {

/**
 * Record write position for creating index stream
 */
class PositionRecorder {
 public:
  virtual ~PositionRecorder() = default;
  virtual void add(uint64_t pos, int32_t entry) = 0;
};

/**
 * A subclass of Google's ZeroCopyOutputStream that supports output to memory
 * buffer, and flushing to OutputStream.
 * By extending Google's class, we get the ability to pass it directly
 * to the protobuf writers.
 */
class BufferedOutputStream : public google::protobuf::io::ZeroCopyOutputStream {
 public:
  explicit BufferedOutputStream(DataBufferHolder& bufferHolder)
      : bufferHolder_{bufferHolder}, buffer_{bufferHolder_.getMemoryPool()} {}

  bool Next(void** data, int32_t* size) override {
    return Next(data, size, 1);
  }

  void BackUp(int32_t count) override;

  google::protobuf::int64 ByteCount() const override {
    return static_cast<google::protobuf::int64>(size());
  }

  bool WriteAliasedRaw(const void* /* unused */, int32_t /* unused */)
      override {
    DWIO_RAISE("WriteAliasedRaw is not supported");
    return false;
  }

  bool AllowsAliasing() const override {
    return false;
  }

  // Similar to Next(data, size), but in addition, allows callers to specify
  // expected increment so buffer allocation is more efficient
  virtual bool Next(void** data, int32_t* size, uint64_t increment);

  virtual uint64_t flush();

  virtual std::string getName() const {
    return folly::to<std::string>("BufferedOutputStream, size: ", size());
  }

  virtual uint64_t size() const {
    // buffered size + flushed size
    return buffer_.size() + bufferHolder_.size();
  }

  virtual void recordPosition(
      PositionRecorder& recorder,
      int32_t bufferLength,
      int32_t bufferOffset,
      int32_t strideIndex = -1) const {
    auto streamSize = size();
    if (streamSize) {
      streamSize -= (bufferLength - bufferOffset);
    }
    recorder.add(streamSize, strideIndex);
  }

 protected:
  DataBufferHolder& bufferHolder_;
  dwio::common::DataBuffer<char> buffer_;

  // try increase buffer size, and then assign to output buffer/size. Returns
  // false if buffer size remained the same
  bool tryResize(
      void** buffer,
      int32_t* size,
      uint64_t headerSize,
      uint64_t increment) {
    auto origSize = buffer_.size();
    if (bufferHolder_.tryResize(buffer_, headerSize, increment)) {
      // if original buffer is empty. need to adjust buffer position/size to
      // accommodate header
      if (UNLIKELY(!origSize && headerSize)) {
        origSize = headerSize;
      }
      *buffer = buffer_.data() + origSize;
      *size = static_cast<int32_t>(buffer_.size() - origSize);
      return true;
    } else {
      return false;
    }
  }

  // this method is called after tryResize() returns false
  void flushAndReset(
      void** buffer,
      int32_t* size,
      uint64_t headerSize,
      const std::vector<folly::StringPiece>& bufferToFlush) {
    bufferHolder_.take(bufferToFlush);
    *buffer = buffer_.data() + headerSize;
    *size = static_cast<int32_t>(buffer_.size() - headerSize);
  }
};

/**
 * An append only buffered stream that allows
 * buffer, and flushing to OutputStream.
 * By extending Google's class, we get the ability to pass it directly
 * to the protobuf writers.
 */
class AppendOnlyBufferedStream {
 private:
  std::unique_ptr<BufferedOutputStream> outStream_;
  char* buffer_{nullptr};
  int32_t bufferLength_{0};
  int32_t bufferOffset_{0};

 public:
  explicit AppendOnlyBufferedStream(
      std::unique_ptr<BufferedOutputStream> outStream)
      : outStream_(std::move(outStream)) {
    DWIO_ENSURE_NOT_NULL(outStream_);
  }

  void write(const char* data, size_t size);
  uint64_t flush();

  uint64_t size() const {
    // size of written content, not size of allocated. So need to subtract size
    // that is not in use
    return outStream_->size() - bufferLength_ + bufferOffset_;
  }

  void recordPosition(PositionRecorder& recorder, int32_t strideOffset = -1)
      const {
    outStream_->recordPosition(
        recorder, bufferLength_, bufferOffset_, strideOffset);
  }
};

} // namespace facebook::velox::dwrf
