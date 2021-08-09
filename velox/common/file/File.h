/*
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
//
// We provide a registration method for read and write files so the appropriate
// type of file can be constructed based on a filename. See the
// (register|generate)ReadFile and (register|generate)WriteFile functions.

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
#include "velox/common/memory/Arena.h"

namespace facebook::velox {

// A read-only file.
class ReadFile {
 public:
  virtual ~ReadFile() {}

  // Returns data at [offset, offset + length). Illegal to pass
  // offset + length > size(). The returned string view will remain valid for at
  // last as long as the |arena| remains uncleared and *this remains valid.
  virtual std::string_view pread(uint64_t offset, uint64_t length, Arena* arena)
      const = 0;

  // Same as above, but places the read data into the provided pre-allocated
  // buffer. Unlike the above call, we guarantee the bytes the returned
  // string_view points to are actually copied into |buf|.
  virtual std::string_view pread(uint64_t offset, uint64_t length, void* buf)
      const = 0;

  // Same as above, but returns owned data directly.
  virtual std::string pread(uint64_t offset, uint64_t length) const = 0;

  // Reads starting at 'offset' into the memory referenced by the
  // Ranges in 'buffers'. The buffers are filled left to right. A
  // buffer with nullptr data will cause its size worth of bytes to be skipped.
  virtual uint64_t preadv(
      uint64_t /*offset*/,
      const std::vector<folly::Range<char*>>& /*buffers*/) {
    VELOX_NYI("preadv not supported");
  }

  // Like preadv but may execute asynchronously and returns the read
  // size or exception via SemiFuture. Use hasPreadvAsync() to check
  // if the implementation is in fact asynchronous.
  virtual folly::SemiFuture<uint64_t> preadvAsync(
      uint64_t offset,
      const std::vector<folly::Range<char*>>& buffers) {
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
// is destroyed.
class WriteFile {
 public:
  // Any cleanup (disk flush, etc.) will be done here. There is no explicit
  // Close function.
  virtual ~WriteFile() {}

  // Appends data to the end of the file.
  virtual void append(std::string_view data) = 0;

  // Current file size, i.e. the sum of all previous Appends.
  virtual uint64_t size() const = 0;
};

// We expect that programs will perform these registrations lazily at static
// link time, e.g. via the lazyRegisterFileClass function. This function takes
// a std::function that will itself call the registerFileClass function. We do
// it lazily to get around the fact that many libraries need folly::init to
// have been called before they will work properly. Basically as a user you
// shouldn't have to worry about these register functions -- all you will need
// to use is the generate functions below.
//
// The registration function take three parameters: a
// std::function<bool(std::string_view)> that says whether the registered
// File subclass should be used for that filename, and lambdas that generate
// the actual Read/Write file. Each registered type is tried in the order it was
// registered, so keep this in mind if multiple types could match the same
// filename.
void lazyRegisterFileClass(std::function<void()> lazy_registration);
void registerFileClass(
    std::function<bool(std::string_view)> filenameMatcher,
    std::function<std::unique_ptr<ReadFile>(std::string_view)> readGenerator,
    std::function<std::unique_ptr<WriteFile>(std::string_view)> writeGenerator);

// Returns a read/write file of type appropriate for filename.
std::unique_ptr<ReadFile> generateReadFile(std::string_view filename);
std::unique_ptr<WriteFile> generateWriteFile(std::string_view filename);

// We currently do a simple implementation for the in-memory files
// that simply resizes a string as needed. If there ever gets used in
// a performance sensitive path we'd probably want to move to a Cord-like
// implementation for underlying storage.

// We don't provide registration functions for the in-memory files, as they
// aren't intended for any robust use needing a filesystem.

class InMemoryReadFile final : public ReadFile {
 public:
  explicit InMemoryReadFile(std::string_view file) : file_(file) {}

  std::string_view pread(uint64_t offset, uint64_t length, Arena* arena)
      const final;
  std::string_view pread(uint64_t offset, uint64_t length, void* buf)
      const final;
  std::string pread(uint64_t offset, uint64_t length) const final;
  uint64_t preadv(
      uint64_t offset,
      const std::vector<folly::Range<char*>>& buffers) final;
  uint64_t size() const final {
    return file_.size();
  }
  uint64_t memoryUsage() const final {
    return size();
  }

 private:
  const std::string_view file_;
};

class InMemoryWriteFile final : public WriteFile {
 public:
  explicit InMemoryWriteFile(std::string* file) : file_(file) {}

  void append(std::string_view data) final;
  uint64_t size() const final;

 private:
  std::string* file_;
};

// Current implementation for the local version is quite simple (e.g. no
// internal arenaing), as local disk writes are expected to be cheap. Local
// files match against any filepath starting with '/'.

class LocalReadFile final : public ReadFile {
 public:
  explicit LocalReadFile(std::string_view path);

  std::string_view pread(uint64_t offset, uint64_t length, Arena* arena)
      const final;
  std::string_view pread(uint64_t offset, uint64_t length, void* buf)
      const final;
  std::string pread(uint64_t offset, uint64_t length) const final;
  uint64_t size() const final;
  uint64_t preadv(
      uint64_t offset,
      const std::vector<folly::Range<char*>>& buffers) final;
  uint64_t memoryUsage() const final;

 private:
  void preadInternal(uint64_t offset, uint64_t length, char* pos) const;

  int32_t fd_;
  mutable long size_ = -1;
};

class LocalWriteFile final : public WriteFile {
 public:
  // An error is thrown is a file already exists at |path|.
  explicit LocalWriteFile(std::string_view path);
  ~LocalWriteFile();

  void append(std::string_view data) final;
  uint64_t size() const final;

 private:
  FILE* file_;
  mutable long size_;
};

} // namespace facebook::velox
