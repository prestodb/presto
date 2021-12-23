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

#include "velox/common/base/Exceptions.h"
#include "velox/dwio/common/InputStream.h"
#include "velox/dwio/parquet/reader/duckdb/InputStreamFileHandle.h"
#include "velox/external/duckdb/duckdb.hpp"

#include <folly/Synchronized.h>
#include <folly/container/F14Map.h>

namespace facebook::velox::duckdb {

class InputStreamFileSystem : public ::duckdb::FileSystem {
 public:
  InputStreamFileSystem() {}
  ~InputStreamFileSystem() override = default;

  std::unique_ptr<::duckdb::FileHandle> OpenFile(
      const std::string& path,
      uint8_t /*flags*/,
      ::duckdb::FileLockType /*lock = ::duckdb::FileLockType::NO_LOCK*/,
      ::duckdb::FileCompressionType /*compression =
          ::duckdb::FileCompressionType::UNCOMPRESSED*/
      ,
      ::duckdb::FileOpener* /*opener = nullptr*/) override {
    int streamId = std::stoi(path);

    auto lockedStreams = streams_.wlock();
    auto it = lockedStreams->find(streamId);
    VELOX_CHECK(
        it != lockedStreams->end(), "Unknown stream with ID {}", streamId);
    ++it->second.first;
    return std::make_unique<InputStreamFileHandle>(*this, streamId);
  }

  std::unique_ptr<::duckdb::FileHandle> OpenStream(
      std::unique_ptr<dwio::common::InputStream> stream) {
    auto streamId = nextStreamId_++;
    streams_.wlock()->emplace(
        std::make_pair(streamId, std::make_pair(1, std::move(stream))));
    return std::make_unique<InputStreamFileHandle>(*this, streamId);
  }

  void CloseStream(int streamId) {
    auto lockedStreams = streams_.wlock();
    auto it = lockedStreams->find(streamId);
    VELOX_CHECK(
        it != lockedStreams->end(), "Unknown stream with ID {}", streamId);
    if (it->second.first == 1) {
      lockedStreams->erase(it);
    } else {
      --it->second.first;
    }
  }

  void Read(
      ::duckdb::FileHandle& handle,
      void* buffer,
      int64_t nr_bytes,
      uint64_t location) override {
    auto& streamHandle = dynamic_cast<InputStreamFileHandle&>(handle);
    dwio::common::InputStream* stream = nullptr;
    {
      auto lockedStreams = streams_.rlock();
      auto it = lockedStreams->find(streamHandle.streamId());
      VELOX_CHECK(
          it != lockedStreams->end(),
          "Unknown stream with ID {}",
          streamHandle.streamId());
      stream = it->second.second.get();
    }
    stream->read(buffer, nr_bytes, location, dwio::common::LogType::FILE);
    streamHandle.setOffset(location + nr_bytes);
  }

  void Write(
      ::duckdb::FileHandle& /*handle*/,
      void* /*buffer*/,
      int64_t /*nr_bytes*/,
      uint64_t /*location*/) override {
    VELOX_FAIL("Unexpected call to InputStreamFileSystem::Write");
  }

  int64_t Read(
      ::duckdb::FileHandle& /*handle*/,
      void* /*buffer*/,
      int64_t /*nr_bytes*/) override {
    VELOX_FAIL("Unexpected call to InputStreamFileSystem::Read");
  }

  int64_t Write(
      ::duckdb::FileHandle& /*handle*/,
      void* /*buffer*/,
      int64_t /*nr_bytes*/) override {
    VELOX_FAIL("Unexpected call to InputStreamFileSystem::Write");
  }

  int64_t GetFileSize(::duckdb::FileHandle& handle) override {
    auto& streamHandle = dynamic_cast<InputStreamFileHandle&>(handle);
    dwio::common::InputStream* stream = nullptr;
    {
      auto lockedStreams = streams_.rlock();
      auto it = lockedStreams->find(streamHandle.streamId());
      VELOX_CHECK(
          it != lockedStreams->end(),
          "Unknown stream with ID {}",
          streamHandle.streamId());
      stream = it->second.second.get();
    }
    return stream->getLength();
  }

  time_t GetLastModifiedTime(::duckdb::FileHandle& /*handle*/) override {
    VELOX_FAIL("Unexpected call to InputStreamFileSystem::GetLastModifiedTime");
  }

  void Truncate(::duckdb::FileHandle& /*handle*/, int64_t /*new_size*/)
      override {
    VELOX_FAIL("Unexpected call to InputStreamFileSystem::Truncate");
  }

  bool DirectoryExists(const std::string& /*directory*/) override {
    VELOX_FAIL("Unexpected call to InputStreamFileSystem::DirectoryExists");
  }

  void CreateDirectory(const std::string& /*directory*/) override {
    VELOX_FAIL("Unexpected call to InputStreamFileSystem::CreateDirectory");
  }

  void RemoveDirectory(const std::string& /*directory*/) override {
    VELOX_FAIL("Unexpected call to InputStreamFileSystem::RemoveDirectory");
  }

  bool ListFiles(
      const std::string& /*directory*/,
      const std::function<void(std::string, bool)>& /*callback*/) override {
    VELOX_FAIL("Unexpected call to InputStreamFileSystem::ListFiles");
  }

  void MoveFile(const std::string& /*source*/, const std::string& /*target*/)
      override {
    VELOX_FAIL("Unexpected call to InputStreamFileSystem::MoveFile");
  }

  bool FileExists(const std::string& /*filename*/) override {
    VELOX_FAIL("Unexpected call to InputStreamFileSystem::FileExists");
  }

  void RemoveFile(const std::string& /*filename*/) override {
    VELOX_FAIL("Unexpected call to InputStreamFileSystem::RemoveFile");
  }

  void FileSync(::duckdb::FileHandle& /*handle*/) override {
    VELOX_FAIL("Unexpected call to InputStreamFileSystem::FileSync");
  }

  std::vector<std::string> Glob(const std::string& /*path*/) override {
    VELOX_FAIL("Unexpected call to InputStreamFileSystem::Glob");
  }

  virtual std::string GetName() const override {
    VELOX_FAIL("Unexpected call to InputStreamFileSystem::GetName");
  }

 private:
  int nextStreamId_ = 1;
  // Maps stream ID to file handle counter and input stream.
  folly::Synchronized<folly::F14FastMap<
      int,
      std::pair<int, std::unique_ptr<dwio::common::InputStream>>>>
      streams_;
};

} // namespace facebook::velox::duckdb
