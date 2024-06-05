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

#include <vector>

#include "velox/dwio/common/DataBuffer.h"
#include "velox/dwio/common/InputStream.h"
#include "velox/dwio/common/wrap/zero-copy-stream-wrapper.h"

namespace facebook::velox::dwio::common {

void printBuffer(std::ostream& out, const char* buffer, uint64_t length);

class PositionProvider {
 public:
  explicit PositionProvider(const std::vector<uint64_t>& positions)
      : position_{positions.begin()}, end_{positions.end()} {}

  uint64_t next();

  bool hasNext() const;

 private:
  std::vector<uint64_t>::const_iterator position_;
  std::vector<uint64_t>::const_iterator end_;
};

/**
 * A subclass of Google's ZeroCopyInputStream that supports seek.
 * By extending Google's class, we get the ability to pass it directly
 * to the protobuf readers.
 */
class SeekableInputStream : public google::protobuf::io::ZeroCopyInputStream {
 public:
  ~SeekableInputStream() override = default;

  virtual void seekToPosition(PositionProvider& position) = 0;

  virtual std::string getName() const = 0;

  // Returns the number of position values this input stream uses to identify an
  // ORC/DWRF stream address.
  virtual size_t positionSize() = 0;

  virtual bool SkipInt64(int64_t count) = 0;

  bool Skip(int32_t count) final override {
    return SkipInt64(count);
  }

  void readFully(char* buffer, size_t bufferSize);
};

/**
 * Create a seekable input stream based on a memory range.
 */
class SeekableArrayInputStream : public SeekableInputStream {
 public:
  SeekableArrayInputStream(
      const unsigned char* list,
      uint64_t length,
      uint64_t block_size = 0);
  SeekableArrayInputStream(
      const char* list,
      uint64_t length,
      uint64_t block_size = 0);
  // Same as above, but takes ownership of the underlying data.
  SeekableArrayInputStream(
      std::unique_ptr<char[]> list,
      uint64_t length,
      uint64_t block_size = 0);

  explicit SeekableArrayInputStream(
      std::function<std::tuple<const char*, uint64_t>()> dataRead,
      uint64_t block_size = 0);

  ~SeekableArrayInputStream() override = default;

  virtual bool Next(const void** data, int32_t* size) override;
  virtual void BackUp(int32_t count) override;
  virtual bool SkipInt64(int64_t count) override;
  virtual google::protobuf::int64 ByteCount() const override;
  virtual void seekToPosition(PositionProvider& position) override;
  virtual std::string getName() const override;
  virtual size_t positionSize() override;

  /// Return the total number of bytes returned from Next() calls.  Intended to
  /// be used for test validation.
  int64_t totalRead() const {
    return totalRead_;
  }

 private:
  void loadIfAvailable();

  // data may optionally be owned by *this via ownedData.
  const std::unique_ptr<char[]> ownedData_;
  const char* data_;
  std::function<std::tuple<const char*, uint64_t>()> dataRead_;
  uint64_t length_;
  uint64_t position_;
  uint64_t blockSize_;
  int64_t totalRead_ = 0;
};

/**
 * Create a seekable input stream based on an io stream.
 */
class SeekableFileInputStream : public SeekableInputStream {
 public:
  SeekableFileInputStream(
      std::shared_ptr<ReadFileInputStream> input,
      uint64_t offset,
      uint64_t byteCount,
      memory::MemoryPool& pool,
      LogType logType,
      uint64_t blockSize = 0);
  ~SeekableFileInputStream() override = default;

  virtual bool Next(const void** data, int32_t* size) override;
  virtual void BackUp(int32_t count) override;
  virtual bool SkipInt64(int64_t count) override;
  virtual google::protobuf::int64 ByteCount() const override;
  virtual void seekToPosition(PositionProvider& position) override;
  virtual std::string getName() const override;
  virtual size_t positionSize() override;

 private:
  const std::shared_ptr<ReadFileInputStream> input_;
  const LogType logType_;
  const uint64_t start_;
  const uint64_t length_;
  const uint64_t blockSize_;
  memory::MemoryPool* const pool_;

  DataBuffer<char> buffer_;
  uint64_t position_;
  uint64_t pushback_;
};

} // namespace facebook::velox::dwio::common
