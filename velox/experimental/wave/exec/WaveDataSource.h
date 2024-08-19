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

#include "velox/common/time/Timer.h"
#include "velox/connectors/Connector.h"
#include "velox/exec/Task.h"
#include "velox/experimental/wave/exec/WaveOperator.h"
#include "velox/expression/Expr.h"

namespace facebook::velox::wave {

class WaveSplitReader;

/// A delegate produced by a regular Velox connector::DataSource for reading its
/// particular file format on GPU. Same methods, except that Wave schedule() and
/// related are exposed instead of an iterator model.
class WaveDataSource : public std::enable_shared_from_this<WaveDataSource> {
 public:
  virtual ~WaveDataSource() = default;

  /// Sets the operand ids of the subfields that are projected out.
  void setOutputOperands(const DefinesMap& defines) {
    defines_ = &defines;
  }

  virtual void addDynamicFilter(
      column_index_t outputChannel,
      const std::shared_ptr<common::Filter>& filter) = 0;

  virtual void addSplit(std::shared_ptr<connector::ConnectorSplit> split) = 0;

  virtual int32_t canAdvance(WaveStream& stream) = 0;

  virtual void schedule(WaveStream& stream, int32_t maxRows = 0) = 0;

  virtual bool isFinished() = 0;

  virtual std::shared_ptr<WaveSplitReader> splitReader() = 0;

  virtual uint64_t getCompletedBytes() = 0;

  virtual uint64_t getCompletedRows() = 0;

  virtual std::unordered_map<std::string, RuntimeCounter> runtimeStats() = 0;

  virtual void setFromDataSource(std::shared_ptr<WaveDataSource> source) {
    VELOX_UNSUPPORTED();
  }

 protected:
  const DefinesMap* defines_{nullptr};
};

} // namespace facebook::velox::wave
