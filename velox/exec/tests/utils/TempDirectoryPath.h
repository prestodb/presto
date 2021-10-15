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
#include <memory>
#include <string>

#include "velox/common/base/Exceptions.h"

namespace facebook::velox::exec::test {

// It manages the lifetime of a temporary directory.
class TempDirectoryPath {
 public:
  static std::shared_ptr<TempDirectoryPath> create();

  virtual ~TempDirectoryPath();

  const std::string path;

  TempDirectoryPath(const TempDirectoryPath&) = delete;
  TempDirectoryPath& operator=(const TempDirectoryPath&) = delete;

  TempDirectoryPath() : path(createTempDirectory()) {}

  static std::string createTempDirectory() {
    char path[] = "/tmp/velox_test_XXXXXX";
    const char* tempDirectoryPath = mkdtemp(path);
    if (tempDirectoryPath == nullptr) {
      throw std::logic_error("Cannot open temp directory");
    }
    return tempDirectoryPath;
  }
};
} // namespace facebook::velox::exec::test
