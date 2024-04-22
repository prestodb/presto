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

#include "velox/exec/tests/utils/TempFilePath.h"

namespace facebook::velox::exec::test {

TempFilePath::~TempFilePath() {
  ::unlink(tempPath_.c_str());
  ::close(fd_);
}

std::shared_ptr<TempFilePath> TempFilePath::create(bool enableFaultInjection) {
  auto* tempFilePath = new TempFilePath(enableFaultInjection);
  return std::shared_ptr<TempFilePath>(tempFilePath);
}

std::string TempFilePath::createTempFile(TempFilePath* tempFilePath) {
  char path[] = "/tmp/velox_test_XXXXXX";
  tempFilePath->fd_ = ::mkstemp(path);
  if (tempFilePath->fd_ == -1) {
    VELOX_FAIL("Cannot open temp file: {}", folly::errnoStr(errno));
  }
  return path;
}
} // namespace facebook::velox::exec::test
