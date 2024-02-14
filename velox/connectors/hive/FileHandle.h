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

// A FileHandle is a File pointer plus some (optional, file-type-dependent)
// extra information for speeding up loading columnar data. For example, when
// we open a file we might build a hash map saying what region(s) on disk
// correspond to a given column in a given stripe.
//
// The FileHandle will normally be used in conjunction with a CachedFactory
// to speed up queries that hit the same files repeatedly; see the
// FileHandleCache and FileHandleFactory.

#pragma once

#include "velox/common/caching/CachedFactory.h"
#include "velox/common/caching/FileIds.h"
#include "velox/common/file/File.h"

namespace facebook::velox {

class Config;

// See the file comment.
struct FileHandle {
  std::shared_ptr<ReadFile> file;

  // Each time we make a new FileHandle we assign it a uuid and use that id as
  // the identifier in downstream data caching structures. This saves a lot of
  // memory compared to using the filename as the identifier.
  StringIdLease uuid;

  // Id for the group of files this belongs to, e.g. its
  // directory. Used for coarse granularity access tracking, for
  // example to decide placing on SSD.
  StringIdLease groupId;

  // We'll want to have a hash map here to record the identifier->byte range
  // mappings. Different formats may have different identifiers, so we may need
  // a union of maps. For example in orc you need 3 integers (I think, to be
  // confirmed with xldb): the row bundle, the node, and the sequence. For the
  // first diff we'll not include the map.
};

using FileHandleCache = SimpleLRUCache<std::string, FileHandle>;

// Creates FileHandles via the Generator interface the CachedFactory requires.
class FileHandleGenerator {
 public:
  FileHandleGenerator() {}
  FileHandleGenerator(std::shared_ptr<const Config> properties)
      : properties_(std::move(properties)) {}
  std::shared_ptr<FileHandle> operator()(const std::string& filename);

 private:
  const std::shared_ptr<const Config> properties_;
};

using FileHandleFactory = CachedFactory<
    std::string,
    std::shared_ptr<FileHandle>,
    FileHandleGenerator>;

using FileHandleCacheStats = SimpleLRUCacheStats;

} // namespace facebook::velox
