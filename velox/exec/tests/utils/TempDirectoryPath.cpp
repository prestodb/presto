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

#include "velox/exec/tests/utils/TempDirectoryPath.h"

#include "boost/filesystem.hpp"

namespace facebook::velox::exec::test {

std::shared_ptr<TempDirectoryPath> TempDirectoryPath::create(bool injectFault) {
  auto* tempDirPath = new TempDirectoryPath(injectFault);
  return std::shared_ptr<TempDirectoryPath>(tempDirPath);
}

TempDirectoryPath::~TempDirectoryPath() {
  LOG(INFO) << "TempDirectoryPath:: removing all files from " << tempPath_;
  try {
    boost::filesystem::remove_all(tempPath_.c_str());
  } catch (...) {
    LOG(WARNING)
        << "TempDirectoryPath:: destructor failed while calling boost::filesystem::remove_all";
  }
}

std::string TempDirectoryPath::createTempDirectory() {
  char tempPath[] = "/tmp/velox_test_XXXXXX";
  const char* tempDirectoryPath = ::mkdtemp(tempPath);
  VELOX_CHECK_NOT_NULL(tempDirectoryPath, "Cannot open temp directory");
  return tempDirectoryPath;
}

} // namespace facebook::velox::exec::test
