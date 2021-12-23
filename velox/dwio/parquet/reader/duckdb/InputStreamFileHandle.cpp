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

#include "velox/dwio/parquet/reader/duckdb/InputStreamFileSystem.h"

namespace facebook::velox::duckdb {

InputStreamFileHandle::~InputStreamFileHandle() {
  Close();
}

void InputStreamFileHandle::Close() {
  if (streamId_) {
    dynamic_cast<InputStreamFileSystem&>(file_system).CloseStream(streamId_);
    streamId_ = 0;
  }
}

} // namespace facebook::velox::duckdb
