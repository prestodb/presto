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

#include "velox/dwio/common/FileSink.h"

namespace facebook::velox::text {

/// Takes character(s) and writes into a 'sink'.
/// It buffers the characters(s) in memory before flushing to the sink.
/// The upper limit character count is specified by 'flushBufferSize'.
class BufferedWriterSink {
 public:
  BufferedWriterSink(
      std::unique_ptr<dwio::common::FileSink> sink,
      std::shared_ptr<memory::MemoryPool> pool,
      uint64_t flushBufferSize);

  ~BufferedWriterSink();

  void write(char value);
  void write(const char* data, uint64_t size);
  void flush();
  /// Discard the data in buffer and close the buffer and fileSink.
  void abort();
  /// Flush the data in buffer to fileSink and close the buffer and fileSink.
  void close();

 private:
  void reserveBuffer();

  const std::unique_ptr<dwio::common::FileSink> sink_;
  const std::shared_ptr<memory::MemoryPool> pool_;
  // The buffer size limit and triggers flush if exceeds this limit.
  const uint64_t flushBufferSize_;
  const std::unique_ptr<dwio::common::DataBuffer<char>> buf_;
  // TODO: add a flag to indicate sink is aborted to prevent flush and write
  // operations after aborted
};

} // namespace facebook::velox::text
