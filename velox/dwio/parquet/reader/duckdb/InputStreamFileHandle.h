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

#include "velox/external/duckdb/duckdb.hpp"

namespace facebook::velox::duckdb {

class InputStreamFileSystem;

class InputStreamFileHandle : public ::duckdb::FileHandle {
 public:
  InputStreamFileHandle(::duckdb::FileSystem& fileSystem, int streamId)
      : FileHandle(fileSystem, std::to_string(streamId)),
        streamId_(streamId),
        offset_(0) {}

  ~InputStreamFileHandle() override;

  int streamId() const {
    return streamId_;
  }

  uint64_t offset() const {
    return offset_;
  }

  void setOffset(uint64_t newOffset) {
    offset_ = newOffset;
  }

 protected:
  void Close() override;

 private:
  int streamId_;
  uint64_t offset_;
};

} // namespace facebook::velox::duckdb
