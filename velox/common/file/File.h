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

// Abstraction of a simplified file interface.
//
// Implementations are available in this file for local disk and in-memory.
//
// We implement only a small subset of the normal file operations, namely
// Append for writing data and PRead for reading data.
//
// All functions are not threadsafe -- external locking is required, even
// for const member functions.

#pragma once

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <string>
#include <string_view>

#include <folly/Range.h>
#include <folly/futures/Future.h>

#include "velox/common/base/Exceptions.h"
#include "velox/common/file/Region.h"

namespace facebook::velox {

// A read-only file.  All methods in this object should be thread safe.
class ReadFile {
 public:
  virtual ~ReadFile() = default;

  // Reads the data at [offset, offset + length) into the provided pre-allocated
  // buffer 'buf'. The bytes are returned as a string_view pointing to 'buf'.
  //
  // This method should be thread safe.
  virtual std::string_view pread(uint64_t offset, uint64_t length, void* buf)
      const = 0;

  // Same as above, but returns owned data directly.
  //
  // This method should be thread safe.
  virtual std::string pread(uint64_t offset, uint64_t length) const;

  // Reads starting at 'offset' into the memory referenced by the
  // Ranges in 'buffers'. The buffers are filled left to right. A
  // buffer with nullptr data will cause its size worth of bytes to be skipped.
  //
  // This method should be thread safe.
  virtual uint64_t preadv(
      uint64_t /*offset*/,
      const std::vector<folly::Range<char*>>& /*buffers*/) const;

  // Vectorized read API. Implementations can coalesce and parallelize.
  // The offsets don't need to be sorted.
  // `iobufs` is a range of IOBufs to store the read data. They
  // will be stored in the same order as the input `regions` vector. So the
  // array must be pre-allocated by the caller, with the same size as `regions`,
  // but don't need to be initialized, since each iobuf will be copy-constructed
  // by the preadv.
  //
  // This method should be thread safe.
  virtual void preadv(
      folly::Range<const common::Region*> regions,
      folly::Range<folly::IOBuf*> iobufs) const;

  /// Like preadv but may execute asynchronously and returns the read size or
  /// exception via SemiFuture. Use hasPreadvAsync() to check if the
  /// implementation is in fact asynchronous.
  ///
  /// This method should be thread safe.
  virtual folly::SemiFuture<uint64_t> preadvAsync(
      uint64_t offset,
      const std::vector<folly::Range<char*>>& buffers) const {
    try {
      return folly::SemiFuture<uint64_t>(preadv(offset, buffers));
    } catch (const std::exception& e) {
      return folly::makeSemiFuture<uint64_t>(e);
    }
  }

  // Returns true if preadvAsync has a native implementation that is
  // asynchronous. The default implementation is synchronous.
  virtual bool hasPreadvAsync() const {
    return false;
  }

  // Whether preads should be coalesced where possible. E.g. remote disk would
  // set to true, in-memory to false.
  virtual bool shouldCoalesce() const = 0;

  // Number of bytes in the file.
  virtual uint64_t size() const = 0;

  // An estimate for the total amount of memory *this uses.
  virtual uint64_t memoryUsage() const = 0;

  // The total number of bytes *this had been used to read since creation or
  // the last resetBytesRead. We sum all the |length| variables passed to
  // preads, not the actual amount of bytes read (which might be less).
  virtual uint64_t bytesRead() const {
    return bytesRead_;
  }

  virtual void resetBytesRead() {
    bytesRead_ = 0;
  }

  virtual std::string getName() const = 0;

  /// Gets the natural size for reads. Returns the number of bytes that should
  /// be read at once.
  virtual uint64_t getNaturalReadSize() const = 0;

 protected:
  mutable std::atomic<uint64_t> bytesRead_ = 0;
};

// A write-only file. Nothing written to the file should be read back until it
// is closed.
class WriteFile {
 public:
  virtual ~WriteFile() = default;

  // Appends data to the end of the file.
  virtual void append(std::string_view data) = 0;

  // Appends data to the end of the file.
  virtual void append(std::unique_ptr<folly::IOBuf> /* data */) {
    VELOX_NYI("IOBuf appending is not implemented");
  }

  // Flushes any local buffers, i.e. ensures the backing medium received
  // all data that has been appended.
  virtual void flush() = 0;

  // Close the file. Any cleanup (disk flush, etc.) will be done here.
  virtual void close() = 0;

  /// Current file size, i.e. the sum of all previous Appends.  No flush should
  /// be needed to get the exact size written, and this should be able to be
  /// called after the file close.
  virtual uint64_t size() const = 0;
};

// We currently do a simple implementation for the in-memory files
// that simply resizes a string as needed. If there ever gets used in
// a performance sensitive path we'd probably want to move to a Cord-like
// implementation for underlying storage.

// We don't provide registration functions for the in-memory files, as they
// aren't intended for any robust use needing a filesystem.

class InMemoryReadFile : public ReadFile {
 public:
  explicit InMemoryReadFile(std::string_view file) : file_(file) {}

  explicit InMemoryReadFile(std::string file)
      : ownedFile_(std::move(file)), file_(ownedFile_) {}

  std::string_view pread(uint64_t offset, uint64_t length, void* buf)
      const override;

  std::string pread(uint64_t offset, uint64_t length) const override;

  uint64_t size() const final {
    return file_.size();
  }

  uint64_t memoryUsage() const final {
    return size();
  }

  // Mainly for testing. Coalescing isn't helpful for in memory data.
  void setShouldCoalesce(bool shouldCoalesce) {
    shouldCoalesce_ = shouldCoalesce;
  }
  bool shouldCoalesce() const final {
    return shouldCoalesce_;
  }

  std::string getName() const override {
    return "<InMemoryReadFile>";
  }

  uint64_t getNaturalReadSize() const override {
    return 1024;
  }

 private:
  const std::string ownedFile_;
  const std::string_view file_;
  bool shouldCoalesce_ = false;
};

class InMemoryWriteFile final : public WriteFile {
 public:
  explicit InMemoryWriteFile(std::string* file) : file_(file) {}

  void append(std::string_view data) final;
  void append(std::unique_ptr<folly::IOBuf> data) final;
  void flush() final {}
  void close() final {}
  uint64_t size() const final;

 private:
  std::string* file_;
};

/// Current implementation for the local version is quite simple (e.g. no
/// internal arenaing), as local disk writes are expected to be cheap. Local
/// files match against any filepath starting with '/'.
class LocalReadFile final : public ReadFile {
 public:
  explicit LocalReadFile(std::string_view path);

  /// TODO: deprecate this after creating local file all through velox fs
  /// interface.
  explicit LocalReadFile(int32_t fd);

  ~LocalReadFile();

  std::string_view pread(uint64_t offset, uint64_t length, void* buf)
      const final;

  uint64_t size() const final;

  uint64_t preadv(
      uint64_t offset,
      const std::vector<folly::Range<char*>>& buffers) const final;

  uint64_t memoryUsage() const final;

  bool shouldCoalesce() const final {
    return false;
  }

  std::string getName() const override {
    if (path_.empty()) {
      return "<LocalReadFile>";
    }
    return path_;
  }

  uint64_t getNaturalReadSize() const override {
    return 10 << 20;
  }

 private:
  void preadInternal(uint64_t offset, uint64_t length, char* pos) const;

  std::string path_;
  int32_t fd_;
  long size_;
};

class LocalWriteFile final : public WriteFile {
 public:
  // An error is thrown is a file already exists at |path|,
  // unless flag shouldThrowOnFileAlreadyExists is false
  explicit LocalWriteFile(
      std::string_view path,
      bool shouldCreateParentDirectories = false,
      bool shouldThrowOnFileAlreadyExists = true);
  ~LocalWriteFile();

  void append(std::string_view data) final;
  void append(std::unique_ptr<folly::IOBuf> data) final;
  void flush() final;
  void close() final;

  uint64_t size() const final {
    return size_;
  }

 private:
  FILE* file_;
  uint64_t size_{0};
  bool closed_{false};
};

} // namespace facebook::velox
