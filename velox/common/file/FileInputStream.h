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

#include <cstdint>

#include "velox/buffer/Buffer.h"
#include "velox/common/file/File.h"
#include "velox/common/memory/ByteStream.h"

namespace facebook::velox::common {

/// Readonly byte input stream backed by file.
class FileInputStream : public ByteInputStream {
 public:
  FileInputStream(
      std::unique_ptr<ReadFile>&& file,
      uint64_t bufferSize,
      memory::MemoryPool* pool);

  ~FileInputStream() override;

  FileInputStream(const FileInputStream&) = delete;
  FileInputStream& operator=(const FileInputStream& other) = delete;
  FileInputStream(FileInputStream&& other) noexcept = delete;
  FileInputStream& operator=(FileInputStream&& other) noexcept = delete;

  size_t size() const override;

  bool atEnd() const override;

  std::streampos tellp() const override;

  void seekp(std::streampos pos) override;

  void skip(int32_t size) override;

  size_t remainingSize() const override;

  uint8_t readByte() override;

  void readBytes(uint8_t* bytes, int32_t size) override;

  std::string_view nextView(int32_t size) override;

  std::string toString() const override;

  /// Records the file read stats.
  struct Stats {
    uint32_t numReads{0};
    uint64_t readBytes{0};
    uint64_t readTimeNs{0};

    bool operator==(const Stats& other) const;

    std::string toString() const;
  };
  Stats stats() const;

 private:
  void doSeek(int64_t skipBytes);

  // Invoked to read the next byte range from the file in a buffer.
  void readNextRange();

  // Issues readahead if underlying file system supports async mode read.
  //
  // TODO: we might consider to use AsyncSource to support read-ahead on
  // filesystem which doesn't support async mode read.
  void maybeIssueReadahead();

  inline uint64_t readSize() const;

  inline uint32_t bufferIndex() const {
    return bufferIndex_;
  }

  inline uint32_t nextBufferIndex() const {
    return (bufferIndex_ + 1) % buffers_.size();
  }

  // Advances buffer index to point to the next buffer for read.
  inline void advanceBuffer() {
    bufferIndex_ = nextBufferIndex();
  }

  inline Buffer* buffer() const {
    return buffers_[bufferIndex()].get();
  }

  inline Buffer* nextBuffer() const {
    return buffers_[nextBufferIndex()].get();
  }

  void updateStats(uint64_t readBytes, uint64_t readTimeNs);

  const std::unique_ptr<ReadFile> file_;
  const uint64_t fileSize_;
  const uint64_t bufferSize_;
  memory::MemoryPool* const pool_;
  const bool readAheadEnabled_;

  // Offset of the next byte to read from file.
  uint64_t fileOffset_ = 0;

  std::vector<BufferPtr> buffers_;
  uint32_t bufferIndex_{0};
  // Sets to read-ahead future if valid.
  folly::SemiFuture<uint64_t> readAheadWait_{
      folly::SemiFuture<uint64_t>::makeEmpty()};

  Stats stats_;
};
} // namespace facebook::velox::common
