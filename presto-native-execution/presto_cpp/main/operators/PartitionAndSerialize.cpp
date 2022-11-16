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
#include "presto_cpp/main/operators/PartitionAndSerialize.h"
#include "velox/connectors/hive/HivePartitionFunction.h"
#include "velox/row/UnsafeRowDynamicSerializer.h"

using namespace facebook::velox::exec;
using namespace facebook::velox;

namespace facebook::presto::operators {

void PartitionAndSerializeNode::addDetails(std::stringstream& stream) const {
  stream << "(";
  for (auto i = 0; i < keys_.size(); ++i) {
    const auto& expr = keys_[i];
    if (i > 0) {
      stream << ", ";
    }
    if (auto field =
            std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(expr)) {
      stream << field->name();
    } else if (
        auto constant =
            std::dynamic_pointer_cast<const core::ConstantTypedExpr>(expr)) {
      stream << constant->toString();
    } else {
      stream << expr->toString();
    }
  }
  stream << ") " << numPartitions_;
}

namespace {

/// The output of this operator has 2 columns:
/// (1) partition number (INTEGER);
/// (2) serialized row (VARBINARY)
class PartitionAndSerializeOperator : public Operator {
 public:
  PartitionAndSerializeOperator(
      int32_t operatorId,
      DriverCtx* FOLLY_NONNULL ctx,
      const std::shared_ptr<const PartitionAndSerializeNode>& planNode)
      : Operator(
            ctx,
            planNode->outputType(),
            operatorId,
            planNode->id(),
            "PartitionAndSerialize"),
        partitionFunction_(
            planNode->partitionFunctionFactory()(planNode->numPartitions())) {}

  bool needsInput() const override {
    return !input_;
  }

  void addInput(RowVectorPtr input) override {
    input_ = std::move(input);
  }

  RowVectorPtr getOutput() override {
    if (!input_) {
      return nullptr;
    }

    const auto numInput = input_->size();

    // TODO Reuse output vector.
    auto output = std::dynamic_pointer_cast<RowVector>(
        BaseVector::create(outputType_, numInput, pool()));

    computePartitions(*output->childAt(0)->asFlatVector<int32_t>());

    serializeRows(*output->childAt(1)->asFlatVector<StringView>());

    input_.reset();

    return output;
  }

  BlockingReason isBlocked(ContinueFuture* future) override {
    return BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return noMoreInput_;
  }

 private:
  void computePartitions(FlatVector<int32_t>& partitionsVector) {
    auto numInput = input_->size();

    partitions_.resize(numInput);
    partitionFunction_->partition(*input_, partitions_);

    // TODO Avoid copy.
    partitionsVector.resize(numInput);
    auto rawPartitions = partitionsVector.mutableRawValues();
    ::memcpy(rawPartitions, partitions_.data(), sizeof(int32_t) * numInput);
  }

  void serializeRows(FlatVector<StringView>& dataVector) {
    const auto numInput = input_->size();

    dataVector.resize(numInput);

    // Compute row sizes.
    rowSizes_.resize(numInput);

    size_t totalSize = 0;
    for (auto i = 0; i < numInput; ++i) {
      const size_t rowSize = velox::row::UnsafeRowDynamicSerializer::getSizeRow(
          input_->type(), input_.get(), i);
      rowSizes_[i] = rowSize;
      totalSize += rowSize;
    }

    // Allocate memory.
    auto buffer = dataVector.getBufferWithSpace(totalSize);

    // Serialize rows.
    auto rawBuffer = buffer->asMutable<char>();
    size_t offset = 0;
    for (auto i = 0; i < numInput; ++i) {
      dataVector.setNoCopy(i, StringView(rawBuffer + offset, rowSizes_[i]));

      // Write row data.
      auto size = velox::row::UnsafeRowDynamicSerializer::serialize(
                      input_->type(), input_, rawBuffer + offset, i)
                      .value_or(0);
      VELOX_DCHECK_EQ(size, rowSizes_[i]);
      offset += size;
    }
  }

  std::unique_ptr<core::PartitionFunction> partitionFunction_;
  std::vector<uint32_t> partitions_;
  std::vector<size_t> rowSizes_;
};
} // namespace

std::unique_ptr<Operator> PartitionAndSerializeTranslator::toOperator(
    DriverCtx* ctx,
    int32_t id,
    const core::PlanNodePtr& node) {
  if (auto partitionNode =
          std::dynamic_pointer_cast<const PartitionAndSerializeNode>(node)) {
    return std::make_unique<PartitionAndSerializeOperator>(
        id, ctx, partitionNode);
  }
  return nullptr;
}
} // namespace facebook::presto::operators
