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

#include "velox/core/PlanNode.h"
#include "velox/exec/Operator.h"

namespace facebook::velox::exec {

/**
 * The class implements a simple table writer VELOX operator
 */
class TableWriter : public Operator {
 public:
  TableWriter(
      int32_t operatorId,
      DriverCtx* driverCtx,
      const std::shared_ptr<const core::TableWriteNode>& tableWriteNode);

  BlockingReason isBlocked(ContinueFuture* /* future */) override {
    return BlockingReason::kNotBlocked;
  }

  void addInput(RowVectorPtr input) override;

  void noMoreInput() override {
    Operator::noMoreInput();
    close();
  }

  virtual bool needsInput() const override {
    return true;
  }

  void close() override {
    if (!closed_) {
      if (dataSink_) {
        dataSink_->close();
      }
      closed_ = true;
    }
  }

  RowVectorPtr getOutput() override;

  bool isFinished() override {
    return finished_;
  }

 private:
  void createDataSink();

  std::vector<ChannelIndex> inputMapping_;
  std::shared_ptr<const RowType> mappedType_;
  vector_size_t numWrittenRows_;
  bool finished_;
  bool closed_;
  DriverCtx* driverCtx_;
  std::shared_ptr<connector::Connector> connector_;
  std::shared_ptr<connector::ConnectorQueryCtx> connectorQueryCtx_;
  std::shared_ptr<connector::DataSink> dataSink_;
  std::shared_ptr<connector::ConnectorInsertTableHandle> insertTableHandle_;
};
} // namespace facebook::velox::exec
