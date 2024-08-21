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

#include "velox/common/file/FileInputStream.h"
#include "velox/common/file/FileSystems.h"
#include "velox/core/PlanNode.h"
#include "velox/core/QueryCtx.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/vector/VectorStream.h"

namespace facebook::velox::exec::trace {

class QueryDataReader {
 public:
  explicit QueryDataReader(std::string path, memory::MemoryPool* pool);

  /// Reads from 'dataStream_' and deserializes to 'batch'. Returns false if
  /// reaches to end of the stream and 'batch' is set to nullptr.
  bool read(RowVectorPtr& batch) const;

 private:
  RowTypePtr getTraceDataType() const;

  std::unique_ptr<common::FileInputStream> getDataInputStream() const;

  const std::string path_;
  const serializer::presto::PrestoVectorSerde::PrestoOptions readOptions_{
      true,
      common::CompressionKind_ZSTD, // TODO: Use trace config.
      /*nullsFirst=*/true};
  const std::shared_ptr<filesystems::FileSystem> fs_;
  memory::MemoryPool* const pool_;
  const RowTypePtr dataType_;
  const std::unique_ptr<common::FileInputStream> dataStream_;
};
} // namespace facebook::velox::exec::trace
