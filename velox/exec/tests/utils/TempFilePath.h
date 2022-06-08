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

#include <unistd.h>
#include <cstdlib>
#include <fstream>
#include <memory>
#include <string>

#include "velox/common/base/Exceptions.h"

namespace facebook::velox::exec::test {

// It manages the lifetime of a temporary file.
class TempFilePath {
 public:
  static std::shared_ptr<TempFilePath> create();

  virtual ~TempFilePath() {
    unlink(path.c_str());
    close(fd);
  }

  const std::string path;

  TempFilePath(const TempFilePath&) = delete;
  TempFilePath& operator=(const TempFilePath&) = delete;

  void append(std::string data) {
    std::ofstream file(path, std::ios_base::app);
    file << data;
    file.flush();
    file.close();
  }

 private:
  int fd;

  TempFilePath() : path(createTempFile(this)) {
    VELOX_CHECK_NE(fd, -1);
  }

  static std::string createTempFile(TempFilePath* tempFilePath) {
    char path[] = "/tmp/velox_test_XXXXXX";
    tempFilePath->fd = mkstemp(path);
    if (tempFilePath->fd == -1) {
      throw std::logic_error("Cannot open temp file");
    }
    return path;
  }
};

} // namespace facebook::velox::exec::test
