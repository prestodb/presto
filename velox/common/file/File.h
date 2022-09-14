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

namespace facebook::velox {

// A read-only file.
class ReadFile {
 public:
  virtual ~ReadFile() = default;

  // Reads the data at [offset, offset + length) into the provided pre-allocated
  // buffer 'buf'. The bytes are returned as a string_view pointing to 'buf'.
  virtual std::string_view
  pread(uint64_t offset, uint64_t length, void* FOLLY_NONNULL buf) const = 0;

  // Same as above, but returns owned data directly.
  virtual std::string pread(uint64_t offset, uint64_t length) const = 0;

  // Reads starting at 'offset' into the memory referenced by the
  // Ranges in 'buffers'. The buffers are filled left to right. A
  // buffer with nullptr data will cause its size worth of bytes to be skipped.
  virtual uint64_t preadv(
      uint64_t /*offset*/,
      const std::vector<folly::Range<char*>>& /*buffers*/) const {
    VELOX_NYI("preadv not supported");
  }

  // Like preadv but may execute asynchronously and returns the read
  // size or exception via SemiFuture. Use hasPreadvAsync() to check
  // if the implementation is in fact asynchronous.
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

  // Flushes any local buffers, i.e. ensures the backing medium received
  // all data that has been appended.
  virtual void flush() = 0;

  // Close the file. Any cleanup (disk flush, etc.) will be done here.
  virtual void close() = 0;

  // Current file size, i.e. the sum of all previous Appends.
  virtual uint64_t size() const = 0;
};

// We currently do a simple implementation for the in-memory files
// that simply resizes a string as needed. If there ever gets used in
// a performance sensitive path we'd probably want to move to a Cord-like
// implementation for underlying storage.

// We don't provide registration functions for the in-memory files, as they
// aren't intended for any robust use needing a filesystem.

class InMemoryReadFile final : public ReadFile {
 public:
  explicit InMemoryReadFile(std::string_view file) : file_(file) {}

  explicit InMemoryReadFile(std::string file)
      : ownedFile_(std::move(file)), file_(ownedFile_) {}

  std::string_view
  pread(uint64_t offset, uint64_t length, void* FOLLY_NONNULL buf) const final;

  std::string pread(uint64_t offset, uint64_t length) const final;

  uint64_t preadv(
      uint64_t offset,
      const std::vector<folly::Range<char*>>& buffers) const final;

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

 private:
  const std::string ownedFile_;
  const std::string_view file_;
  bool shouldCoalesce_ = false;
};

class InMemoryWriteFile final : public WriteFile {
 public:
  explicit InMemoryWriteFile(std::string* FOLLY_NONNULL file) : file_(file) {}

  void append(std::string_view data) final;
  void flush() final {}
  void close() final {}
  uint64_t size() const final;

 private:
  std::string* FOLLY_NONNULL file_;
};

// Current implementation for the local version is quite simple (e.g. no
// internal arenaing), as local disk writes are expected to be cheap. Local
// files match against any filepath starting with '/'.

class LocalReadFile final : public ReadFile {
 public:
  explicit LocalReadFile(std::string_view path);

  explicit LocalReadFile(int32_t fd);

  std::string_view
  pread(uint64_t offset, uint64_t length, void* FOLLY_NONNULL buf) const final;

  std::string pread(uint64_t offset, uint64_t length) const final;

  uint64_t size() const final;

  uint64_t preadv(
      uint64_t offset,
      const std::vector<folly::Range<char*>>& buffers) const final;

  uint64_t memoryUsage() const final;

  bool shouldCoalesce() const final {
    return false;
  }

 private:
  void preadInternal(uint64_t offset, uint64_t length, char* FOLLY_NONNULL pos)
      const;

  int32_t fd_;
  long size_;
};

class LocalWriteFile final : public WriteFile {
 public:
  // An error is thrown is a file already exists at |path|.
  explicit LocalWriteFile(std::string_view path);
  ~LocalWriteFile();

  void append(std::string_view data) final;
  void flush() final;
  void close() final;
  uint64_t size() const final;

 private:
  FILE* FOLLY_NONNULL file_;
  mutable long size_;
  bool closed_{false};
};

} // namespace facebook::velox
