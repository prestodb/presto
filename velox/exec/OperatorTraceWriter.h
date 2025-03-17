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

#include "velox/common/base/TraceConfig.h"
#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"
#include "velox/exec/Split.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/vector/VectorStream.h"

namespace facebook::velox::exec {
class Operator;
}

namespace facebook::velox::exec::trace {

/// Used to serialize and write the input vectors from a particular operator
/// into a data file. Additionally, it creates a corresponding summary file that
/// contains information such as peak memory, input rows, operator type, etc.
class OperatorTraceInputWriter {
 public:
  /// 'traceOp' is the operator to trace. 'traceDir' specifies the trace
  /// directory for the operator.
  explicit OperatorTraceInputWriter(
      Operator* traceOp,
      std::string traceDir,
      memory::MemoryPool* pool,
      UpdateAndCheckTraceLimitCB updateAndCheckTraceLimitCB);

  /// Serializes rows and writes out each batch.
  void write(const RowVectorPtr& rows);

  /// Closes the data file and writes out the data summary.
  void finish();

 private:
  // Flushes the trace data summaries to the disk.
  void writeSummary() const;

  Operator* const traceOp_;
  const std::string traceDir_;
  // TODO: make 'useLosslessTimestamp' configuerable.
  const serializer::presto::PrestoVectorSerde::PrestoOptions options_ = {
      true,
      common::CompressionKind::CompressionKind_ZSTD,
      0.8,
      /*nullsFirst=*/true};
  const std::shared_ptr<filesystems::FileSystem> fs_;
  memory::MemoryPool* const pool_;
  VectorSerde* const serde_;
  const UpdateAndCheckTraceLimitCB updateAndCheckTraceLimitCB_;

  std::unique_ptr<WriteFile> traceFile_;
  std::unique_ptr<VectorStreamGroup> batch_;
  bool finished_{false};
};

/// Used to write the input splits during the execution of a traced 'TableScan'
/// operator. Additionally, it creates a corresponding summary file that
/// contains information such as peak memory, number of splits, etc.
///
/// Currently, it only works with 'HiveConnectorSplit'. In the future, it will
/// be extended to handle more types of splits, such as
/// 'IcebergHiveConnectorSplit'.
class OperatorTraceSplitWriter {
 public:
  explicit OperatorTraceSplitWriter(Operator* traceOp, std::string traceDir);

  /// Serializes and writes out each split. Each serialized split is immediately
  /// flushed to ensure that we can still replay a traced operator even if a
  /// crash occurs during execution.
  void write(const exec::Split& split) const;

  void finish();

 private:
  static std::unique_ptr<folly::IOBuf> serialize(const std::string& split);

  // Flushes the trace data summaries to the disk.
  void writeSummary() const;

  Operator* const traceOp_;
  const std::string traceDir_;
  const std::shared_ptr<filesystems::FileSystem> fs_;
  const std::unique_ptr<WriteFile> splitFile_;
  bool finished_{false};
};
} // namespace facebook::velox::exec::trace
