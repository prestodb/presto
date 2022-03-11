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

#include "velox/exec/TableWriter.h"
#include "velox/exec/Task.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::exec {

TableWriter::TableWriter(
    int32_t operatorId,
    DriverCtx* driverCtx,
    const std::shared_ptr<const core::TableWriteNode>& tableWriteNode)
    : Operator(
          driverCtx,
          tableWriteNode->outputType(),
          operatorId,
          tableWriteNode->id(),
          "TableWrite"),
      numWrittenRows_(0),
      finished_(false),
      closed_(false),
      driverCtx_(driverCtx),
      insertTableHandle_(
          tableWriteNode->insertTableHandle()->connectorInsertTableHandle()) {
  // For the time being the table writer supports one file
  // To extend it we can create a new data sink for each
  // partition
  const auto& connectorId = tableWriteNode->insertTableHandle()->connectorId();
  connector_ = connector::getConnector(connectorId);
  connectorQueryCtx_ =
      operatorCtx_->createConnectorQueryCtx(connectorId, stats_.planNodeId);

  auto names = tableWriteNode->columnNames();
  auto types = tableWriteNode->columns()->children();

  const auto& inputType = tableWriteNode->sources()[0]->outputType();

  inputMapping_.reserve(types.size());
  for (const auto& name : tableWriteNode->columns()->names()) {
    inputMapping_.emplace_back(inputType->getChildIdx(name));
  }

  mappedType_ = ROW(std::move(names), std::move(types));
  const auto& config = driverCtx_->task->queryCtx()->config();
  if (config.createEmptyFiles()) {
    LOG(INFO) << "Enforce creating empty files\n";
    createDataSink();
  }
}

void TableWriter::createDataSink() {
  dataSink_ = connector_->createDataSink(
      mappedType_, insertTableHandle_, connectorQueryCtx_.get());
}

void TableWriter::addInput(RowVectorPtr input) {
  if (input->size() == 0) {
    return;
  }

  std::vector<VectorPtr> mappedChildren;
  mappedChildren.reserve(inputMapping_.size());
  for (auto i : inputMapping_) {
    mappedChildren.emplace_back(input->childAt(i));
  }

  auto mappedInput = std::make_shared<RowVector>(
      input->pool(),
      mappedType_,
      input->nulls(),
      input->size(),
      mappedChildren,
      input->getNullCount());

  // Lazily instantiate data sink to prevent leftover empty files.
  if (!dataSink_) {
    createDataSink();
  }
  dataSink_->appendData(mappedInput);
  numWrittenRows_ += input->size();
}

RowVectorPtr TableWriter::getOutput() {
  // Making sure the output is read only once after the write is fully done
  if (!noMoreInput_ || finished_) {
    return nullptr;
  }
  finished_ = true;

  auto rowsWritten = std::dynamic_pointer_cast<FlatVector<int64_t>>(
      BaseVector::create(BIGINT(), 1, pool()));
  rowsWritten->set(0, numWrittenRows_);

  std::vector<VectorPtr> columns = {rowsWritten};

  // TODO Find a way to not have this Presto-specific logic in here.
  if (outputType_->size() > 1) {
    auto fragments = std::dynamic_pointer_cast<FlatVector<StringView>>(
        BaseVector::create(VARBINARY(), 1, pool()));
    fragments->setNull(0, true);
    columns.emplace_back(fragments);

    // clang-format off
    auto commitContextJson = folly::toJson(
        folly::dynamic::object
            ("lifespan", "TaskWide")
            ("taskId", driverCtx_->task->taskId())
            ("pageSinkCommitStrategy", "NO_COMMIT")
            ("lastPage", false));
    // clang-format on

    auto commitContext = std::make_shared<ConstantVector<StringView>>(
        pool(), 1, false, VARBINARY(), StringView(commitContextJson));
    columns.emplace_back(commitContext);
  }

  return std::make_shared<RowVector>(
      pool(), outputType_, BufferPtr(nullptr), 1, columns);
}
} // namespace facebook::velox::exec
