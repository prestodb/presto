/*
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
#include "presto_cpp/main/operators/IcebergMergeProcessorOperator.h"

#include "velox/connectors/hive/iceberg/IcebergMergeProcessor.h"
#include "velox/exec/OperatorUtils.h"

using namespace facebook::velox::exec;
using namespace facebook::velox;

namespace facebook::presto::operators {
namespace {

// Single-input, single-output Velox operator that delegates the per-row
// MERGE / UPDATE fan-out to
// `velox::connector::hive::iceberg::IcebergMergeProcessor`. The processor is
// stateless; this operator just buffers one input page at a time, calls
// `transform`, and yields the resulting page once.
class IcebergMergeProcessorOperator : public Operator {
 public:
  IcebergMergeProcessorOperator(
      int32_t operatorId,
      DriverCtx* FOLLY_NONNULL ctx,
      const std::shared_ptr<const IcebergMergeProcessorNode>& planNode)
      : Operator(
            ctx,
            planNode->outputType(),
            operatorId,
            planNode->id(),
            "IcebergMergeProcessor"),
        processor_(
            std::make_unique<
                velox::connector::hive::iceberg::IcebergMergeProcessor>(
                planNode->targetColumnTypes(),
                planNode->outputColumnNames(),
                planNode->rowIdType(),
                planNode->targetRowIdChannel(),
                planNode->mergeRowChannel())) {}

  bool needsInput() const override {
    return !pendingOutput_ && !noMoreInput_;
  }

  void addInput(RowVectorPtr input) override {
    VELOX_CHECK_NULL(pendingOutput_);
    auto transformed = processor_->transform(input, operatorCtx_->pool());
    // Velox `Operator::getOutput()` contract requires nullptr (not an empty
    // RowVector) when there is no output for an input page. The processor
    // legitimately returns a zero-row vector when every input row is a
    // DEFAULT_CASE (op == -1) — e.g. all-non-matching MERGE rows — so drop
    // empty results here rather than forwarding them to `getOutput()`.
    if (transformed != nullptr && transformed->size() > 0) {
      pendingOutput_ = std::move(transformed);
    }
  }

  RowVectorPtr getOutput() override {
    if (!pendingOutput_) {
      return nullptr;
    }
    auto output = std::move(pendingOutput_);
    pendingOutput_ = nullptr;
    return output;
  }

  BlockingReason isBlocked(ContinueFuture* /*future*/) override {
    return BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return noMoreInput_ && !pendingOutput_;
  }

 private:
  std::unique_ptr<velox::connector::hive::iceberg::IcebergMergeProcessor>
      processor_;
  // Holds the per-input transformed page until getOutput() flushes it.
  RowVectorPtr pendingOutput_;
};
} // namespace

std::unique_ptr<Operator> IcebergMergeProcessorTranslator::toOperator(
    DriverCtx* ctx,
    int32_t id,
    const core::PlanNodePtr& node) {
  if (auto mergeNode =
          std::dynamic_pointer_cast<const IcebergMergeProcessorNode>(node)) {
    return std::make_unique<IcebergMergeProcessorOperator>(id, ctx, mergeNode);
  }
  return nullptr;
}

void IcebergMergeProcessorNode::addDetails(std::stringstream& stream) const {
  stream << "(targetColumns=" << targetColumnTypes_.size()
         << ", rowIdChannel=" << targetRowIdChannel_
         << ", mergeRowChannel=" << mergeRowChannel_ << ")";
}

velox::RowTypePtr IcebergMergeProcessorNode::buildOutputType(
    const std::vector<velox::TypePtr>& targetColumnTypes,
    const std::vector<std::string>& outputColumnNames,
    const velox::TypePtr& rowIdType) {
  VELOX_USER_CHECK_EQ(
      outputColumnNames.size(),
      targetColumnTypes.size() + 3,
      "IcebergMergeProcessorNode outputColumnNames size ({}) must equal "
      "targetColumnTypes.size() + 3 ({})",
      outputColumnNames.size(),
      targetColumnTypes.size() + 3);
  std::vector<std::string> names;
  std::vector<velox::TypePtr> types;
  names.reserve(targetColumnTypes.size() + 3);
  types.reserve(targetColumnTypes.size() + 3);
  for (size_t i = 0; i < targetColumnTypes.size(); ++i) {
    // Use the iceberg-schema target column names so downstream Velox nodes
    // (e.g. TableWriter::setTypeMappings) that resolve columns by name see
    // the iceberg-correct names rather than synthetic positional placeholders.
    names.push_back(outputColumnNames[i]);
    types.push_back(targetColumnTypes[i]);
  }
  // Trailing three columns: operation TINYINT, rowId, insert_from_update
  // TINYINT — names from the planner output list, types fixed by the
  // IcebergMergeProcessor contract.
  names.push_back(outputColumnNames[targetColumnTypes.size()]);
  types.push_back(velox::TINYINT());
  names.push_back(outputColumnNames[targetColumnTypes.size() + 1]);
  types.push_back(rowIdType);
  names.push_back(outputColumnNames[targetColumnTypes.size() + 2]);
  types.push_back(velox::TINYINT());
  return velox::ROW(std::move(names), std::move(types));
}

folly::dynamic IcebergMergeProcessorNode::serialize() const {
  auto obj = PlanNode::serialize();
  folly::dynamic targetTypes = folly::dynamic::array();
  for (const auto& t : targetColumnTypes_) {
    targetTypes.push_back(t->serialize());
  }
  obj["targetColumnTypes"] = std::move(targetTypes);
  folly::dynamic outputNames = folly::dynamic::array();
  for (const auto& n : outputColumnNames_) {
    outputNames.push_back(n);
  }
  obj["outputColumnNames"] = std::move(outputNames);
  obj["rowIdType"] = rowIdType_->serialize();
  obj["targetRowIdChannel"] = targetRowIdChannel_;
  obj["mergeRowChannel"] = mergeRowChannel_;
  obj["sources"] = ISerializable::serialize(sources_);
  return obj;
}

velox::core::PlanNodePtr IcebergMergeProcessorNode::create(
    const folly::dynamic& obj,
    void* context) {
  std::vector<velox::TypePtr> targetColumnTypes;
  if (obj.count("targetColumnTypes")) {
    const auto& arr = obj["targetColumnTypes"];
    targetColumnTypes.reserve(arr.size());
    for (const auto& t : arr) {
      targetColumnTypes.push_back(ISerializable::deserialize<Type>(t, context));
    }
  }
  std::vector<std::string> outputColumnNames;
  if (obj.count("outputColumnNames")) {
    const auto& arr = obj["outputColumnNames"];
    outputColumnNames.reserve(arr.size());
    for (const auto& n : arr) {
      outputColumnNames.push_back(n.asString());
    }
  }
  auto rowIdType = ISerializable::deserialize<Type>(obj["rowIdType"], context);
  auto sources = ISerializable::deserialize<std::vector<velox::core::PlanNode>>(
      obj["sources"], context);
  return std::make_shared<IcebergMergeProcessorNode>(
      obj["id"].asString(),
      std::move(targetColumnTypes),
      std::move(outputColumnNames),
      std::move(rowIdType),
      static_cast<velox::column_index_t>(obj["targetRowIdChannel"].asInt()),
      static_cast<velox::column_index_t>(obj["mergeRowChannel"].asInt()),
      sources[0]);
}

} // namespace facebook::presto::operators
