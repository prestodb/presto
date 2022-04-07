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
#include "velox/exec/Operator.h"
#include "velox/exec/Driver.h"
#include "velox/exec/OperatorUtils.h"
#include "velox/exec/Task.h"

#include "velox/common/process/ProcessBase.h"
#include "velox/expression/Expr.h"

namespace facebook::velox::exec {
namespace {
// Basic implementation of the connector::ExpressionEvaluator interface.
class SimpleExpressionEvaluator : public connector::ExpressionEvaluator {
 public:
  explicit SimpleExpressionEvaluator(core::ExecCtx* execCtx)
      : execCtx_(execCtx) {}

  std::unique_ptr<exec::ExprSet> compile(
      const std::shared_ptr<const core::ITypedExpr>& expression)
      const override {
    auto expressions = {expression};
    return std::make_unique<exec::ExprSet>(std::move(expressions), execCtx_);
  }

  void evaluate(
      exec::ExprSet* exprSet,
      const SelectivityVector& rows,
      RowVectorPtr& input,
      VectorPtr* result) const override {
    exec::EvalCtx context(execCtx_, exprSet, input.get());

    std::vector<VectorPtr> results = {*result};
    exprSet->eval(0, 1, true, rows, &context, &results);

    *result = results[0];
  }

 private:
  core::ExecCtx* execCtx_;
};
} // namespace

OperatorCtx::OperatorCtx(DriverCtx* driverCtx)
    : driverCtx_(driverCtx), pool_(driverCtx_->addOperatorPool()) {}

core::ExecCtx* OperatorCtx::execCtx() const {
  if (!execCtx_) {
    execCtx_ = std::make_unique<core::ExecCtx>(
        pool_, driverCtx_->task->queryCtx().get());
  }
  return execCtx_.get();
}

std::unique_ptr<connector::ConnectorQueryCtx>
OperatorCtx::createConnectorQueryCtx(
    const std::string& connectorId,
    const std::string& planNodeId) const {
  if (!expressionEvaluator_) {
    expressionEvaluator_ =
        std::make_unique<SimpleExpressionEvaluator>(execCtx());
  }
  return std::make_unique<connector::ConnectorQueryCtx>(
      pool_,
      driverCtx_->task->queryCtx()->getConnectorConfig(connectorId),
      expressionEvaluator_.get(),
      driverCtx_->task->queryCtx()->mappedMemory(),
      fmt::format("{}.{}", driverCtx_->task->taskId(), planNodeId));
}

std::vector<std::unique_ptr<Operator::PlanNodeTranslator>>&
Operator::translators() {
  static std::vector<std::unique_ptr<PlanNodeTranslator>> translators;
  return translators;
}

// static
std::unique_ptr<Operator> Operator::fromPlanNode(
    DriverCtx* ctx,
    int32_t id,
    const std::shared_ptr<const core::PlanNode>& planNode) {
  for (auto& translator : translators()) {
    auto op = translator->translate(ctx, id, planNode);
    if (op) {
      return op;
    }
  }
  return nullptr;
}

// static
void Operator::registerOperator(
    std::unique_ptr<PlanNodeTranslator> translator) {
  translators().emplace_back(std::move(translator));
}

std::optional<uint32_t> Operator::maxDrivers(
    const std::shared_ptr<const core::PlanNode>& planNode) {
  for (auto& translator : translators()) {
    auto current = translator->maxDrivers(planNode);
    if (current) {
      return current;
    }
  }
  return std::nullopt;
}

memory::MappedMemory* OperatorCtx::mappedMemory() const {
  if (!mappedMemory_) {
    mappedMemory_ =
        driverCtx_->task->addOperatorMemory(pool_->getMemoryUsageTracker());
  }
  return mappedMemory_;
}

const std::string& OperatorCtx::taskId() const {
  return driverCtx_->task->taskId();
}

VectorPtr Operator::getResultVector(ChannelIndex index) {
  // If there is a singly referenced results vector and it has a
  // singly referenced flat child with singly referenced buffers, the
  // child can be reused.
  if (!output_ || !output_.unique()) {
    return nullptr;
  }

  VectorPtr& vector = output_->childAt(index);
  if (!vector) {
    return nullptr;
  }

  if (BaseVector::isReusableFlatVector(vector)) {
    vector->resize(0);
    return std::move(vector);
  }

  return nullptr;
}

void Operator::getResultVectors(std::vector<VectorPtr>* result) {
  if (resultProjections_.empty()) {
    return;
  }
  result->resize(resultProjections_.back().inputChannel + 1);
  for (auto& projection : resultProjections_) {
    (*result)[projection.inputChannel] =
        getResultVector(projection.outputChannel);
  }
}

static bool isSequence(
    const vector_size_t* numbers,
    vector_size_t start,
    vector_size_t end) {
  for (vector_size_t i = start; i < end; ++i) {
    if (numbers[i] != i) {
      return false;
    }
  }
  return true;
}

RowVectorPtr Operator::fillOutput(vector_size_t size, BufferPtr mapping) {
  bool wrapResults = true;
  if (size == input_->size() &&
      (!mapping || isSequence(mapping->as<vector_size_t>(), 0, size))) {
    if (isIdentityProjection_) {
      return std::move(input_);
    }
    wrapResults = false;
  }

  if (output_.unique()) {
    output_->resize(size);
  } else {
    std::vector<VectorPtr> localColumns(outputType_->size());
    output_ = std::make_shared<RowVector>(
        operatorCtx_->pool(),
        outputType_,
        BufferPtr(nullptr),
        size,
        std::move(localColumns),
        0 /*nullCount*/);
  }
  auto& columns = output_->children();
  if (!identityProjections_.empty()) {
    auto input = input_->children();
    for (auto& projection : identityProjections_) {
      columns[projection.outputChannel] = wrapResults
          ? wrapChild(size, mapping, input[projection.inputChannel])
          : input[projection.inputChannel];
    }
  }
  for (auto& projection : resultProjections_) {
    columns[projection.outputChannel] = wrapResults
        ? wrapChild(size, mapping, std::move(results_[projection.inputChannel]))
        : std::move(results_[projection.inputChannel]);
  }
  return output_;
}

void Operator::inputProcessed() {
  input_ = nullptr;
  if (!output_.unique()) {
    output_ = nullptr;
    return;
  }
  auto& columns = output_->children();
  for (auto& projection : identityProjections_) {
    columns[projection.outputChannel] = nullptr;
  }
}

void Operator::clearIdentityProjectedOutput() {
  if (!output_ || !output_.unique()) {
    return;
  }
  for (auto& projection : identityProjections_) {
    output_->childAt(projection.outputChannel) = nullptr;
  }
}

void Operator::recordBlockingTime(uint64_t start) {
  uint64_t now =
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::high_resolution_clock::now().time_since_epoch())
          .count();
  stats_.blockedWallNanos += (now - start) * 1000;
}

std::string Operator::toString() const {
  std::stringstream out;
  if (auto task = operatorCtx_->task()) {
    auto driverCtx = operatorCtx_->driverCtx();
    out << stats_.operatorType << "(" << stats_.operatorId << ")<"
        << task->taskId() << ":" << driverCtx->pipelineId << "."
        << driverCtx->driverId << " " << this;
  } else {
    out << "<Terminated, no task>";
  }
  return out.str();
}

std::vector<ChannelIndex> toChannels(
    const RowTypePtr& rowType,
    const std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>>&
        fields) {
  std::vector<ChannelIndex> channels;
  channels.reserve(fields.size());
  for (const auto& field : fields) {
    auto channel = exprToChannel(field.get(), rowType);
    channels.push_back(channel);
  }
  return channels;
}

ChannelIndex exprToChannel(const core::ITypedExpr* expr, const TypePtr& type) {
  if (auto field = dynamic_cast<const core::FieldAccessTypedExpr*>(expr)) {
    return type->as<TypeKind::ROW>().getChildIdx(field->name());
  }
  if (dynamic_cast<const core::ConstantTypedExpr*>(expr)) {
    return kConstantChannel;
  }
  VELOX_CHECK(false, "Expression must be field access or constant");
  return 0; // not reached.
}

std::vector<ChannelIndex> calculateOutputChannels(
    const RowTypePtr& inputType,
    const RowTypePtr& outputType) {
  // Note that outputType may have more columns than inputType as some columns
  // can be duplicated.

  bool identicalProjection = inputType->size() == outputType->size();
  const auto& outputNames = outputType->names();

  std::vector<ChannelIndex> outputChannels;
  outputChannels.resize(outputNames.size());
  for (auto i = 0; i < outputNames.size(); i++) {
    outputChannels[i] = inputType->getChildIdx(outputNames[i]);
    if (outputChannels[i] != i) {
      identicalProjection = false;
    }
  }
  if (identicalProjection) {
    outputChannels.clear();
  }
  return outputChannels;
}

void OperatorStats::add(const OperatorStats& other) {
  numSplits += other.numSplits;
  rawInputBytes += other.rawInputBytes;
  rawInputPositions += other.rawInputPositions;

  addInputTiming.add(other.addInputTiming);
  inputBytes += other.inputBytes;
  inputPositions += other.inputPositions;

  getOutputTiming.add(other.getOutputTiming);
  outputBytes += other.outputBytes;
  outputPositions += other.outputPositions;

  physicalWrittenBytes += other.physicalWrittenBytes;

  blockedWallNanos += other.blockedWallNanos;

  finishTiming.add(other.finishTiming);

  memoryStats.add(other.memoryStats);

  for (const auto& stat : other.runtimeStats) {
    runtimeStats[stat.first].merge(stat.second);
  }

  numDrivers += other.numDrivers;
}

void OperatorStats::clear() {
  numSplits = 0;
  rawInputBytes = 0;
  rawInputPositions = 0;

  addInputTiming.clear();
  inputBytes = 0;
  inputPositions = 0;

  getOutputTiming.clear();
  outputBytes = 0;
  outputPositions = 0;

  physicalWrittenBytes = 0;

  blockedWallNanos = 0;

  finishTiming.clear();

  memoryStats.clear();

  runtimeStats.clear();
}

} // namespace facebook::velox::exec
