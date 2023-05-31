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
      driverCtx_(driverCtx),
      connectorPool_(driverCtx_->task->addConnectorPoolLocked(
          planNodeId(),
          driverCtx_->pipelineId,
          driverCtx_->driverId,
          operatorType(),
          tableWriteNode->insertTableHandle()->connectorId())),
      insertTableHandle_(
          tableWriteNode->insertTableHandle()->connectorInsertTableHandle()),
      commitStrategy_(tableWriteNode->commitStrategy()) {
  const auto& connectorId = tableWriteNode->insertTableHandle()->connectorId();
  connector_ = connector::getConnector(connectorId);
  connectorQueryCtx_ = operatorCtx_->createConnectorQueryCtx(
      connectorId, planNodeId(), connectorPool_);

  auto names = tableWriteNode->columnNames();
  auto types = tableWriteNode->columns()->children();

  const auto& inputType = tableWriteNode->sources()[0]->outputType();

  inputMapping_.reserve(types.size());
  for (const auto& name : tableWriteNode->columns()->names()) {
    inputMapping_.emplace_back(inputType->getChildIdx(name));
  }

  mappedType_ = ROW(std::move(names), std::move(types));
  createDataSink();
}

void TableWriter::createDataSink() {
  dataSink_ = connector_->createDataSink(
      mappedType_,
      insertTableHandle_,
      connectorQueryCtx_.get(),
      commitStrategy_);
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

  if (!dataSink_) {
    createDataSink();
  }
  dataSink_->appendData(mappedInput);
  numWrittenRows_ += input->size();
}

RowVectorPtr TableWriter::getOutput() {
  // Making sure the output is read only once after the write is fully done.
  if (!noMoreInput_ || finished_) {
    return nullptr;
  }
  finished_ = true;

  if (outputType_->size() == 0) {
    return nullptr;
  }
  if (outputType_->size() == 1) {
    return std::make_shared<RowVector>(
        pool(),
        outputType_,
        nullptr,
        1,
        std::vector<VectorPtr>{std::make_shared<ConstantVector<int64_t>>(
            pool(), 1, false /*isNull*/, BIGINT(), numWrittenRows_)});
  }

  std::vector<std::string> fragments = dataSink_->finish();

  vector_size_t numOutputRows = fragments.size() + 1;

  // Set rows column.
  FlatVectorPtr<int64_t> writtenRowsVector =
      BaseVector::create<FlatVector<int64_t>>(BIGINT(), numOutputRows, pool());
  writtenRowsVector->set(0, (int64_t)numWrittenRows_);
  for (int idx = 1; idx < numOutputRows; ++idx) {
    writtenRowsVector->setNull(idx, true);
  }

  // Set fragments column.
  FlatVectorPtr<StringView> fragmentsVector =
      BaseVector::create<FlatVector<StringView>>(
          VARBINARY(), numOutputRows, pool());
  fragmentsVector->setNull(0, true);
  for (int i = 1; i < numOutputRows; ++i) {
    fragmentsVector->set(i, StringView(fragments[i - 1]));
  }

  // Set commitcontext column.
  // clang-format off
    auto commitContextJson = folly::toJson(
      folly::dynamic::object
          ("lifespan", "TaskWide")
          ("taskId", connectorQueryCtx_->taskId())
          ("pageSinkCommitStrategy", commitStrategyToString(commitStrategy_))
          ("lastPage", true));
  // clang-format on

  auto commitContextVector = std::make_shared<ConstantVector<StringView>>(
      pool(),
      numOutputRows,
      false /*isNull*/,
      VARBINARY(),
      StringView(commitContextJson));

  std::vector<VectorPtr> columns = {
      writtenRowsVector, fragmentsVector, commitContextVector};

  return std::make_shared<RowVector>(
      pool(), outputType_, nullptr, numOutputRows, columns);
}
} // namespace facebook::velox::exec
