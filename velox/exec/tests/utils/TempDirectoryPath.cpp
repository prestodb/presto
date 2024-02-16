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

std::shared_ptr<TempDirectoryPath> TempDirectoryPath::create() {
  struct SharedTempDirectoryPath : public TempDirectoryPath {
    SharedTempDirectoryPath() : TempDirectoryPath() {}
  };
  return std::make_shared<SharedTempDirectoryPath>();
}

TempDirectoryPath::~TempDirectoryPath() {
  LOG(INFO) << "TempDirectoryPath:: removing all files from " << path;
  try {
    boost::filesystem::remove_all(path.c_str());
  } catch (...) {
    LOG(WARNING)
        << "TempDirectoryPath:: destructor failed while calling boost::filesystem::remove_all";
  }
}

} // namespace facebook::velox::exec::test
