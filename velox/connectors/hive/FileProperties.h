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

#include <cstdint>

namespace facebook::velox {

struct FileProperties {
  std::optional<int64_t> fileSize;
  std::optional<int64_t> modificationTime;
};

} // namespace facebook::velox
