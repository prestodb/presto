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
#include "velox/dwio/parquet/duckdb_reader/duckdb/InputStreamFileHandle.h"
#include "velox/external/duckdb/duckdb.hpp"

#include <folly/Synchronized.h>
#include <folly/container/F14Map.h>

namespace facebook::velox::duckdb {

// Implements the DuckDB FileSystem API on top of dwio::common::InputStream.
// Hence, an instance always supports only a specific path and limited API.
// This class owns the InputStream instance passed from the HiveConnector.
// TODO: Work with DuckDB to directly support a InputStream API.
class InputStreamFileSystem : public ::duckdb::FileSystem {
 public:
  explicit InputStreamFileSystem(
      std::unique_ptr<dwio::common::InputStream> stream)
      : stream_(std::move(stream)) {}

  ~InputStreamFileSystem() override = default;

  // Arguments are not used as this only supports a specific InputStream
  std::unique_ptr<::duckdb::FileHandle> OpenFile(
      const std::string& /* path */,
      uint8_t /*flags*/,
      ::duckdb::FileLockType /*lock = ::duckdb::FileLockType::NO_LOCK*/,
      ::duckdb::FileCompressionType /*compression =
          ::duckdb::FileCompressionType::UNCOMPRESSED*/
      ,
      ::duckdb::FileOpener* /*opener = nullptr*/) override {
    return std::make_unique<InputStreamFileHandle>(*this);
  }

  std::unique_ptr<::duckdb::FileHandle> OpenFile() {
    return std::make_unique<InputStreamFileHandle>(*this);
  }

  void Read(
      ::duckdb::FileHandle& /* handle */,
      void* buffer,
      int64_t nr_bytes,
      uint64_t location) override {
    stream_->read(buffer, nr_bytes, location, dwio::common::LogType::FILE);
  }

  void Write(
      ::duckdb::FileHandle& /*handle*/,
      void* /*buffer*/,
      int64_t /*nr_bytes*/,
      uint64_t /*location*/) override {
    VELOX_NYI();
  }

  int64_t Read(
      ::duckdb::FileHandle& /*handle*/,
      void* /*buffer*/,
      int64_t /*nr_bytes*/) override {
    VELOX_NYI();
  }

  int64_t Write(
      ::duckdb::FileHandle& /*handle*/,
      void* /*buffer*/,
      int64_t /*nr_bytes*/) override {
    VELOX_NYI();
  }

  int64_t GetFileSize(::duckdb::FileHandle& /* handle */) override {
    return stream_->getLength();
  }

  time_t GetLastModifiedTime(::duckdb::FileHandle& /*handle*/) override {
    VELOX_NYI();
  }

  void Truncate(::duckdb::FileHandle& /*handle*/, int64_t /*new_size*/)
      override {
    VELOX_NYI();
  }

  bool DirectoryExists(const std::string& /*directory*/) override {
    VELOX_NYI();
  }

  void CreateDirectory(const std::string& /*directory*/) override {
    VELOX_NYI();
  }

  void RemoveDirectory(const std::string& /*directory*/) override {
    VELOX_NYI();
  }

  bool ListFiles(
      const std::string& /*directory*/,
      const std::function<void(const std::string&, bool)>& /*callback*/)
      override {
    VELOX_NYI();
  }

  void MoveFile(const std::string& /*source*/, const std::string& /*target*/)
      override {
    VELOX_NYI();
  }

  bool FileExists(const std::string& /*filename*/) override {
    VELOX_NYI();
  }

  void RemoveFile(const std::string& /*filename*/) override {
    VELOX_NYI();
  }

  void FileSync(::duckdb::FileHandle& /*handle*/) override {
    VELOX_NYI();
  }

  bool OnDiskFile(::duckdb::FileHandle& /*handle*/) override {
    return false;
  }

  bool CanSeek() override {
    return false;
  }

  std::vector<std::string> Glob(
      const std::string& /*path*/,
      ::duckdb::FileOpener* /*opener*/ = nullptr) override {
    VELOX_NYI();
  }

  virtual std::string GetName() const override {
    return "dwio::InputStream";
  }

 private:
  std::unique_ptr<dwio::common::InputStream> stream_;
};

} // namespace facebook::velox::duckdb
