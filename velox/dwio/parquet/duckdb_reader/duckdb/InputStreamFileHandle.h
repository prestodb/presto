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

#include "velox/external/duckdb/duckdb.hpp"

namespace facebook::velox::duckdb {

class InputStreamFileSystem;

// Implements the DuckDB FileHandle API
class InputStreamFileHandle : public ::duckdb::FileHandle {
 public:
  explicit InputStreamFileHandle(::duckdb::FileSystem& fileSystem)
      : FileHandle(fileSystem, "") {}

  ~InputStreamFileHandle() override {}

 protected:
  void Close() override {}
};

} // namespace facebook::velox::duckdb
