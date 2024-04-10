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

#include <folly/executors/ThreadedExecutor.h>
#include <folly/futures/Future.h>
#include "velox/common/file/File.h"
#include "velox/connectors/hive/storage_adapters/abfs/AbfsUtil.h"

namespace facebook::velox::filesystems::abfs {
class AbfsReadFile final : public ReadFile {
 public:
  explicit AbfsReadFile(const std::string& path, const std::string& connectStr);

  void initialize(const FileOptions& options);

  std::string_view pread(uint64_t offset, uint64_t length, void* buf)
      const final;

  std::string pread(uint64_t offset, uint64_t length) const final;

  uint64_t preadv(
      uint64_t offset,
      const std::vector<folly::Range<char*>>& buffers) const final;

  void preadv(
      folly::Range<const common::Region*> regions,
      folly::Range<folly::IOBuf*> iobufs) const final;

  uint64_t size() const final;

  uint64_t memoryUsage() const final;

  bool shouldCoalesce() const final;

  std::string getName() const final;

  uint64_t getNaturalReadSize() const final;

 protected:
  class Impl;
  std::shared_ptr<Impl> impl_;
};
} // namespace facebook::velox::filesystems::abfs
