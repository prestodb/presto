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

#include <hdfs/hdfs.h>
#include "velox/common/file/File.h"

namespace facebook::velox {

class HdfsReadFile final : public ReadFile {
 public:
  explicit HdfsReadFile(hdfsFS hdfs, std::string_view path);

  std::string_view pread(uint64_t offset, uint64_t length, void* buf)
      const final;

  std::string pread(uint64_t offset, uint64_t length) const final;

  uint64_t size() const final;

  uint64_t memoryUsage() const final;

  bool shouldCoalesce() const final;

 private:
  void preadInternal(uint64_t offset, uint64_t length, char* pos) const;
  void seekToPosition(hdfsFile file, uint64_t offset) const;
  void checkFileReadParameters(uint64_t offset, uint64_t length) const;
  hdfsFS hdfsClient_;
  hdfsFileInfo* fileInfo_;
  std::string filePath_;
};
} // namespace facebook::velox
