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

#include "velox/common/caching/SsdCache.h"
#include "velox/common/caching/SsdFile.h"

namespace facebook::velox::cache::test {

class SsdFileTestHelper {
 public:
  explicit SsdFileTestHelper(SsdFile* ssdFile) : ssdFile_(ssdFile) {}

  uint64_t writeFileSize() {
    return ssdFile_->writeFile_->size();
  }

 private:
  SsdFile* const ssdFile_;
};

class SsdCacheTestHelper {
 public:
  explicit SsdCacheTestHelper(SsdCache* ssdCache) : ssdCache_(ssdCache) {}

  int32_t numShards() {
    return ssdCache_->numShards_;
  }

  uint64_t writeFileSize(uint64_t fileId) {
    return ssdCache_->file(fileId).writeFile_->size();
  }

 private:
  SsdCache* const ssdCache_;
};
} // namespace facebook::velox::cache::test
