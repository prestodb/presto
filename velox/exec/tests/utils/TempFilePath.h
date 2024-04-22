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

#include <sys/stat.h>
#include <unistd.h>
#include <cstdlib>
#include <fstream>
#include <memory>
#include <string>

#include "velox/common/base/Exceptions.h"

namespace facebook::velox::exec::test {

/// Manages the lifetime of a temporary file.
class TempFilePath {
 public:
  /// If 'enableFaultInjection' is true, we enable fault injection on the
  /// created file.
  static std::shared_ptr<TempFilePath> create(
      bool enableFaultInjection = false);

  ~TempFilePath();

  TempFilePath(const TempFilePath&) = delete;
  TempFilePath& operator=(const TempFilePath&) = delete;

  void append(std::string data) {
    std::ofstream file(tempPath_, std::ios_base::app);
    file << data;
    file.flush();
    file.close();
  }

  const int64_t fileSize() {
    struct stat st;
    ::stat(tempPath_.data(), &st);
    return st.st_size;
  }

  int64_t fileModifiedTime() {
    struct stat st;
    ::stat(tempPath_.data(), &st);
    return st.st_mtime;
  }

  /// If fault injection is enabled, the returned the file path has the faulty
  /// file system prefix scheme. The velox fs then opens the file through the
  /// faulty file system. The actual file operation might either fails or
  /// delegate to the actual file.
  const std::string& getPath() const {
    return path_;
  }

  // Returns the delegated file path if fault injection is enabled.
  const std::string& tempFilePath() const {
    return tempPath_;
  }

 private:
  static std::string createTempFile(TempFilePath* tempFilePath);

  TempFilePath(bool enableFaultInjection)
      : enableFaultInjection_(enableFaultInjection),
        tempPath_(createTempFile(this)),
        path_(
            enableFaultInjection_ ? fmt::format("faulty:{}", tempPath_)
                                  : tempPath_) {
    VELOX_CHECK_NE(fd_, -1);
  }

  const bool enableFaultInjection_;
  const std::string tempPath_;
  const std::string path_;

  int fd_;
};

} // namespace facebook::velox::exec::test
