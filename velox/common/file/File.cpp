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

#include "velox/common/file/File.h"
#include "velox/common/base/Fs.h"

#include <fmt/format.h>
#include <glog/logging.h>
#include <memory>
#include <stdexcept>

#include <folly/portability/SysUio.h>
#ifdef linux
#include <linux/fs.h>
#endif // linux
#include <sys/ioctl.h>

namespace facebook::velox {

#define RETURN_IF_ERROR(func, result) \
  result = func;                      \
  if (result < 0) {                   \
    return result;                    \
  }

namespace {
FOLLY_ALWAYS_INLINE void checkNotClosed(bool closed) {
  VELOX_CHECK(!closed, "file is closed");
}

template <typename T>
T getAttribute(
    const std::unordered_map<std::string, std::string>& attributes,
    const std::string_view& key,
    const T& defaultValue) {
  if (attributes.count(std::string(key)) > 0) {
    try {
      return folly::to<T>(attributes.at(std::string(key)));
    } catch (const std::exception& e) {
      VELOX_FAIL("Failed while parsing File attributes: {}", e.what());
    }
  }
  return defaultValue;
}
} // namespace

std::string ReadFile::pread(uint64_t offset, uint64_t length) const {
  std::string buf;
  buf.resize(length);
  auto res = pread(offset, length, buf.data());
  buf.resize(res.size());
  return buf;
}

uint64_t ReadFile::preadv(
    uint64_t offset,
    const std::vector<folly::Range<char*>>& buffers) const {
  auto fileSize = size();
  uint64_t numRead = 0;
  if (offset >= fileSize) {
    return 0;
  }
  for (auto& range : buffers) {
    auto copySize = std::min<size_t>(range.size(), fileSize - offset);
    // NOTE: skip the gap in case of coalesce io.
    if (range.data() != nullptr) {
      pread(offset, copySize, range.data());
    }
    offset += copySize;
    numRead += copySize;
  }
  return numRead;
}

uint64_t ReadFile::preadv(
    folly::Range<const common::Region*> regions,
    folly::Range<folly::IOBuf*> iobufs) const {
  VELOX_CHECK_EQ(regions.size(), iobufs.size());
  uint64_t length = 0;
  for (size_t i = 0; i < regions.size(); ++i) {
    const auto& region = regions[i];
    auto& output = iobufs[i];
    output = folly::IOBuf(folly::IOBuf::CREATE, region.length);
    pread(region.offset, region.length, output.writableData());
    output.append(region.length);
    length += region.length;
  }

  return length;
}

std::string_view
InMemoryReadFile::pread(uint64_t offset, uint64_t length, void* buf) const {
  bytesRead_ += length;
  memcpy(buf, file_.data() + offset, length);
  return {static_cast<char*>(buf), length};
}

std::string InMemoryReadFile::pread(uint64_t offset, uint64_t length) const {
  bytesRead_ += length;
  return std::string(file_.data() + offset, length);
}

void InMemoryWriteFile::append(std::string_view data) {
  file_->append(data);
}

void InMemoryWriteFile::append(std::unique_ptr<folly::IOBuf> data) {
  for (auto rangeIter = data->begin(); rangeIter != data->end(); ++rangeIter) {
    file_->append(
        reinterpret_cast<const char*>(rangeIter->data()), rangeIter->size());
  }
}

uint64_t InMemoryWriteFile::size() const {
  return file_->size();
}

LocalReadFile::LocalReadFile(std::string_view path) : path_(path) {
  fd_ = open(path_.c_str(), O_RDONLY);
  if (fd_ < 0) {
    if (errno == ENOENT) {
      VELOX_FILE_NOT_FOUND_ERROR("No such file or directory: {}", path);
    } else {
      VELOX_FAIL(
          "open failure in LocalReadFile constructor, {} {} {}.",
          fd_,
          path,
          folly::errnoStr(errno));
    }
  }
  const off_t ret = lseek(fd_, 0, SEEK_END);
  VELOX_CHECK_GE(
      ret,
      0,
      "fseek failure in LocalReadFile constructor, {} {} {}.",
      ret,
      path,
      folly::errnoStr(errno));
  size_ = ret;
}

LocalReadFile::LocalReadFile(int32_t fd) : fd_(fd) {}

LocalReadFile::~LocalReadFile() {
  const int ret = close(fd_);
  if (ret < 0) {
    LOG(WARNING) << "close failure in LocalReadFile destructor: " << ret << ", "
                 << folly::errnoStr(errno);
  }
}

void LocalReadFile::preadInternal(uint64_t offset, uint64_t length, char* pos)
    const {
  bytesRead_ += length;
  auto bytesRead = ::pread(fd_, pos, length, offset);
  VELOX_CHECK_EQ(
      bytesRead,
      length,
      "fread failure in LocalReadFile::PReadInternal, {} vs {}.",
      bytesRead,
      length);
}

std::string_view
LocalReadFile::pread(uint64_t offset, uint64_t length, void* buf) const {
  preadInternal(offset, length, static_cast<char*>(buf));
  return {static_cast<char*>(buf), length};
}

uint64_t LocalReadFile::preadv(
    uint64_t offset,
    const std::vector<folly::Range<char*>>& buffers) const {
  // Dropped bytes sized so that a typical dropped range of 50K is not
  // too many iovecs.
  static thread_local std::vector<char> droppedBytes(16 * 1024);
  uint64_t totalBytesRead = 0;
  std::vector<struct iovec> iovecs;
  iovecs.reserve(buffers.size());

  auto readvFunc = [&]() -> ssize_t {
    const auto bytesRead =
        folly::preadv(fd_, iovecs.data(), iovecs.size(), offset);
    if (bytesRead < 0) {
      LOG(ERROR) << "preadv failed with error: " << folly::errnoStr(errno);
    } else {
      totalBytesRead += bytesRead;
      offset += bytesRead;
    }
    iovecs.clear();
    return bytesRead;
  };

  for (auto& range : buffers) {
    if (!range.data()) {
      auto skipSize = range.size();
      while (skipSize) {
        auto bytes = std::min<size_t>(droppedBytes.size(), skipSize);

        if (iovecs.size() >= IOV_MAX) {
          ssize_t bytesRead{0};
          RETURN_IF_ERROR(readvFunc(), bytesRead);
        }

        iovecs.push_back({droppedBytes.data(), bytes});
        skipSize -= bytes;
      }
    } else {
      if (iovecs.size() >= IOV_MAX) {
        ssize_t bytesRead{0};
        RETURN_IF_ERROR(readvFunc(), bytesRead);
      }

      iovecs.push_back({range.data(), range.size()});
    }
  }

  // Perform any remaining preadv calls
  if (!iovecs.empty()) {
    ssize_t bytesRead{0};
    RETURN_IF_ERROR(readvFunc(), bytesRead);
  }

  return totalBytesRead;
}

uint64_t LocalReadFile::size() const {
  return size_;
}

uint64_t LocalReadFile::memoryUsage() const {
  // TODO: does FILE really not use any more memory? From the stdio.h
  // source code it looks like it has only a single integer? Probably
  // we need to go deeper and see how much system memory is being taken
  // by the file descriptor the integer refers to?
  return sizeof(FILE);
}

bool LocalWriteFile::Attributes::cowDisabled(
    const std::unordered_map<std::string, std::string>& attrs) {
  return getAttribute<bool>(attrs, kNoCow, kDefaultNoCow);
}

LocalWriteFile::LocalWriteFile(
    std::string_view path,
    bool shouldCreateParentDirectories,
    bool shouldThrowOnFileAlreadyExists,
    bool bufferWrite)
    : path_(path) {
  const auto dir = fs::path(path_).parent_path();
  if (shouldCreateParentDirectories && !fs::exists(dir)) {
    VELOX_CHECK(
        common::generateFileDirectory(dir.c_str()),
        "Failed to generate file directory");
  }

  // File open flags: write-only, create the file if it doesn't exist.
  int32_t flags = O_WRONLY | O_CREAT;
  if (shouldThrowOnFileAlreadyExists) {
    flags |= O_EXCL;
  }
#ifdef linux
  if (!bufferWrite) {
    flags |= O_DIRECT;
  }
#endif // linux

  // The file mode bits to be applied when a new file is created. By default
  // user has read and write access to the file.
  // NOTE: The mode argument must be supplied if O_CREAT or O_TMPFILE is
  // specified in flags; if it is not supplied, some arbitrary bytes from the
  // stack will be applied as the file mode.
  const int32_t mode = S_IRUSR | S_IWUSR;

  std::unique_ptr<char[]> buf(new char[path_.size() + 1]);
  buf[path_.size()] = 0;
  ::memcpy(buf.get(), path_.data(), path_.size());
  fd_ = open(buf.get(), flags, mode);
  VELOX_CHECK_GE(
      fd_,
      0,
      "Cannot open or create {}. Error: {}",
      path_,
      folly::errnoStr(errno));

  const off_t ret = lseek(fd_, 0, SEEK_END);
  VELOX_CHECK_GE(
      ret,
      0,
      "fseek failure in LocalWriteFile constructor, {} {} {}.",
      ret,
      path_,
      folly::errnoStr(errno));
  size_ = ret;
}

LocalWriteFile::~LocalWriteFile() {
  try {
    close();
  } catch (const std::exception& ex) {
    // We cannot throw an exception from the destructor. Warn instead.
    LOG(WARNING) << "fclose failure in LocalWriteFile destructor: "
                 << ex.what();
  }
}

void LocalWriteFile::append(std::string_view data) {
  checkNotClosed(closed_);
  const uint64_t bytesWritten = ::write(fd_, data.data(), data.size());
  VELOX_CHECK_EQ(
      bytesWritten,
      data.size(),
      "fwrite failure in LocalWriteFile::append, {} vs {}: {}",
      bytesWritten,
      data.size(),
      folly::errnoStr(errno));
  size_ += bytesWritten;
}

void LocalWriteFile::append(std::unique_ptr<folly::IOBuf> data) {
  checkNotClosed(closed_);
  uint64_t totalBytesWritten{0};
  for (auto rangeIter = data->begin(); rangeIter != data->end(); ++rangeIter) {
    const auto bytesToWrite = rangeIter->size();
    const uint64_t bytesWritten =
        ::write(fd_, rangeIter->data(), rangeIter->size());
    totalBytesWritten += bytesWritten;
    if (bytesWritten != bytesToWrite) {
      VELOX_FAIL(
          "fwrite failure in LocalWriteFile::append, {} vs {}: {}",
          bytesWritten,
          bytesToWrite,
          folly::errnoStr(errno));
    }
  }
  const auto totalBytesToWrite = data->computeChainDataLength();
  VELOX_CHECK_EQ(
      totalBytesWritten,
      totalBytesToWrite,
      "Failure in LocalWriteFile::append, {} vs {}",
      totalBytesWritten,
      totalBytesToWrite);
  size_ += totalBytesWritten;
}

void LocalWriteFile::write(
    const std::vector<iovec>& iovecs,
    int64_t offset,
    int64_t length) {
  checkNotClosed(closed_);
  VELOX_CHECK_GE(offset, 0, "Offset cannot be negative.");
  const auto bytesWritten = ::pwritev(
      fd_, iovecs.data(), static_cast<ssize_t>(iovecs.size()), offset);
  VELOX_CHECK_EQ(
      bytesWritten,
      length,
      "Failure in LocalWriteFile::write, {} vs {}",
      bytesWritten,
      length);
  size_ = std::max<uint64_t>(size_, offset + bytesWritten);
}

void LocalWriteFile::truncate(int64_t newSize) {
  checkNotClosed(closed_);
  VELOX_CHECK_GE(newSize, 0, "New size cannot be negative.");
  const auto ret = ::ftruncate(fd_, newSize);
  VELOX_CHECK_EQ(
      ret,
      0,
      "ftruncate failed in LocalWriteFile::truncate: {}.",
      folly::errnoStr(errno));
  size_ = newSize;
}

void LocalWriteFile::flush() {
  checkNotClosed(closed_);
  const auto ret = ::fsync(fd_);
  VELOX_CHECK_EQ(
      ret,
      0,
      "fsync failed in LocalWriteFile::flush: {}.",
      folly::errnoStr(errno));
}

void LocalWriteFile::setAttributes(
    const std::unordered_map<std::string, std::string>& attributes) {
  checkNotClosed(closed_);
  attributes_ = attributes;
#ifdef linux
  if (Attributes::cowDisabled(attributes_)) {
    int attr{0};
    auto ret = ioctl(fd_, FS_IOC_GETFLAGS, &attr);
    VELOX_CHECK_EQ(
        0,
        ret,
        "ioctl(FS_IOC_GETFLAGS) failed: {}, {}",
        ret,
        folly::errnoStr(errno));
    attr |= FS_NOCOW_FL;
    ret = ioctl(fd_, FS_IOC_SETFLAGS, &attr);
    VELOX_CHECK_EQ(
        0,
        ret,
        "ioctl(FS_IOC_SETFLAGS, FS_NOCOW_FL) failed: {}, {}",
        ret,
        folly::errnoStr(errno));
  }
#endif // linux
}

std::unordered_map<std::string, std::string> LocalWriteFile::getAttributes()
    const {
  checkNotClosed(closed_);
  return attributes_;
}

void LocalWriteFile::close() {
  if (!closed_) {
    const auto ret = ::close(fd_);
    VELOX_CHECK_EQ(
        ret,
        0,
        "close failed in LocalWriteFile::close: {}.",
        folly::errnoStr(errno));
    closed_ = true;
  }
}

} // namespace facebook::velox
