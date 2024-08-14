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

#include <chrono>

#include "velox/common/config/Config.h"
#include "velox/common/file/File.h"
#include "velox/common/io/IoStatistics.h"
#include "velox/dwio/common/Closeable.h"
#include "velox/dwio/common/DataBuffer.h"
#include "velox/dwio/common/MetricsLog.h"

namespace facebook::velox::dwio::common {
using namespace facebook::velox::io;

/// An abstract interface for providing a file write sink to different storage
/// system backends.
class FileSink : public Closeable {
 public:
  struct Options {
    /// If true, allows file sink to buffer data before persist to storage.
    bool bufferWrite{true};
    /// Connector properties are required to create a FileSink on FileSystems
    /// such as S3.
    const std::shared_ptr<const config::ConfigBase>& connectorProperties{
        nullptr};
    /// Config used to create sink files. This config is provided to underlying
    /// file system and the config is free form. The form should be defined by
    /// the underlying file system.
    const std::string fileCreateConfig{""};
    memory::MemoryPool* pool{nullptr};
    MetricsLogPtr metricLogger{MetricsLog::voidLog()};
    IoStatistics* stats{nullptr};
  };

  FileSink(std::string name, const Options& options)
      : name_{std::move(name)},
        connectorProperties_{options.connectorProperties},
        pool_(options.pool),
        metricLogger_{options.metricLogger},
        stats_{options.stats},
        size_{0} {}

  ~FileSink() override {
    destroy();
  }

  /// Returns the name of this file sink for error messages.
  ///
  /// NOTE: it is set to the file path on the underlying storage system except
  /// 'WriteFileSink'.
  const std::string& name() const {
    return name_;
  }

  /// Total number of bytes written.
  virtual uint64_t size() const {
    return size_;
  }

  /// Returns true if file sink supports buffering. In the case when buffering
  /// is not supported, caller need to buffer data to yield optimal write size.
  virtual bool isBuffered() const {
    return true;
  }

  /// Writes a single data buffer.
  void write(DataBuffer<char> buffer);

  /// Writes data buffers.
  virtual void write(std::vector<DataBuffer<char>>& buffers) = 0;

  const MetricsLogPtr& metricsLog() const {
    return metricLogger_;
  }

  using Factory = std::function<std::unique_ptr<FileSink>(
      const std::string& name,
      const FileSink::Options& options)>;

  static bool registerFactory(const Factory& factory);

  static std::unique_ptr<FileSink> create(
      const std::string& filePath,
      const Options& options);

 protected:
  // General write wrapper with logging. All concrete subclasses gets logging
  // for free if they call a public method that goes through this method.
  void writeWithLogging(std::vector<DataBuffer<char>>& buffers);

  void writeImpl(
      std::vector<DataBuffer<char>>& buffers,
      const std::function<uint64_t(const DataBuffer<char>&)>& callback);

  const std::string name_;
  const std::shared_ptr<const config::ConfigBase> connectorProperties_;
  memory::MemoryPool* const pool_;
  const MetricsLogPtr metricLogger_;
  IoStatistics* const stats_;

  uint64_t size_;
};

/// Wrapper class that delegates calls to the underlying write file.
class WriteFileSink final : public FileSink {
 public:
  WriteFileSink(
      std::unique_ptr<WriteFile> writeFile,
      std::string name,
      MetricsLogPtr metricLogger = MetricsLog::voidLog(),
      IoStatistics* stats = nullptr);

  ~WriteFileSink() override {
    destroy();
  }

  bool isBuffered() const override {
    return false;
  }

  using FileSink::write;

  void write(std::vector<DataBuffer<char>>& buffers) override;

  void doClose() override;

  // TODO: Hack to make Alpha writer work with Velox.  To be removed after Alpha
  // writer takes DataSink directly.
  std::unique_ptr<WriteFile> toWriteFile() {
    markClosed();
    return std::move(writeFile_);
  }

 private:
  std::unique_ptr<WriteFile> writeFile_;
};

class LocalFileSink : public FileSink {
 public:
  LocalFileSink(const std::string& name, const Options& options);

  ~LocalFileSink() override {
    destroy();
  }

  using FileSink::write;

  void write(std::vector<DataBuffer<char>>& buffers) override;

  static void registerFactory();

 protected:
  void doClose() override {
    ::close(fd_);
  }

 private:
  // The local open file handle.
  int fd_;
};

class MemorySink : public FileSink {
 public:
  MemorySink(size_t capacity, const Options& options);

  ~MemorySink() override {
    markClosed();
  }

  using FileSink::write;

  void write(std::vector<DataBuffer<char>>& buffers) override;

  const char* data() const {
    return data_.data();
  }

  void reset() {
    size_ = 0;
  }

 private:
  DataBuffer<char> data_;
};

void registerFileSinks();

} // namespace facebook::velox::dwio::common

#define VELOX_REGISTER_DATA_SINK_METHOD_DEFINITION(class, function)       \
  void class ::registerFactory() {                                        \
    facebook::velox::dwio::common::FileSink::registerFactory((function)); \
  }
