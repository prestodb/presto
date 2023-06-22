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
#include <folly/lang/Bits.h>
#include "velox/exec/OperatorUtils.h"
#include "velox/row/UnsafeRowFast.h"

using namespace facebook::velox::exec;
using namespace facebook::velox;

namespace facebook::presto::operators {
namespace {
velox::core::PlanNodeId deserializePlanNodeId(const folly::dynamic& obj) {
  return obj["id"].asString();
}

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
        numPartitions_(planNode->numPartitions()),
        serializedRowType_{planNode->serializedRowType()},
        keyChannels_(
            toChannels(planNode->sources()[0]->outputType(), planNode->keys())),
        partitionFunction_(
            numPartitions_ == 1 ? nullptr
                                : planNode->partitionFunctionFactory()->create(
                                      planNode->numPartitions())),
        replicateNullsAndAny_(planNode->isReplicateNullsAndAny()) {
    const auto& inputType = planNode->sources()[0]->outputType()->asRow();
    const auto& serializedRowTypeNames = serializedRowType_->names();
    bool identityMapping = true;
    for (auto i = 0; i < serializedRowTypeNames.size(); ++i) {
      serializedColumnIndices_.push_back(
          inputType.getChildIdx(serializedRowTypeNames[i]));
      if (serializedColumnIndices_.back() != i) {
        identityMapping = false;
      }
    }
    if (identityMapping) {
      serializedColumnIndices_.clear();
    }
  }

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
    collectNullRows();

    const vector_size_t maxOutputSize = calculateMaxOutputSize();

    // TODO Reuse output vector.
    auto output =
        BaseVector::create<RowVector>(outputType_, maxOutputSize, pool());
    auto partitionVector = output->childAt(0)->asFlatVector<int32_t>();
    partitionVector->resize(maxOutputSize);
    auto dataVector = output->childAt(1)->asFlatVector<StringView>();
    dataVector->resize(maxOutputSize);

    computePartitions(*partitionVector);
    serializeRows(*dataVector);

    if (replicateNullsAndAny_ && numPartitions_ > 1) {
      auto numOutputRows =
          replicateRowsWithNullPartitionKeys(*dataVector, *partitionVector);
      // If numOutputRows equal to the input row count, then
      // 'replicateRowsWithNullPartitionKeys' did not insert any row into the
      // output vector, thus we need to call into
      // 'replicateAnyToEmptyPartitions'
      if (numOutputRows == input_->size()) {
        numOutputRows = replicateAnyToEmptyPartitions(
            *partitionVector, *dataVector, numOutputRows);
      }
      partitionVector->resize(numOutputRows);
      dataVector->resize(numOutputRows);
      output->resize(numOutputRows);
    }

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
  // The function replicates all the input rows with null partition keys to all
  // the partitions and returns the total number of output rows after the
  // replication.
  vector_size_t replicateRowsWithNullPartitionKeys(
      FlatVector<StringView>& dataVector,
      FlatVector<int32_t>& partitionVector) {
    auto numInput = input_->size();
    auto nextRow = numInput;

    for (auto partition = 0; partition < numPartitions_; ++partition) {
      for (auto row = 0; row < numInput; ++row) {
        // Replicate a row with null partition key to all the other partitions.
        if (nullRows_.isValid(row) && partitions_[row] != partition) {
          dataVector.setNoCopy(nextRow, dataVector.valueAt(row));
          partitionVector.set(nextRow, partition);
          ++nextRow;
        }
      }
    }
    return nextRow;
  }

  // Replicates the first value to all the empty partitions.
  vector_size_t replicateAnyToEmptyPartitions(
      FlatVector<int32_t>& partitionsVector,
      FlatVector<StringView>& dataVector,
      vector_size_t nextRow) {
    auto* rawPartitions = partitionsVector.mutableRawValues();
    const auto numInput = input_->size();
    std::set<int32_t> nonEmptyPartitions(
        rawPartitions, rawPartitions + numInput);
    StringView anyValue = dataVector.valueAt(0);

    int32_t outputRow = nextRow;
    for (auto idx = 0; idx < numPartitions_; ++idx) {
      // TODO: Optimize so that we don't need the lookup each partitionId
      if (nonEmptyPartitions.find(idx) == nonEmptyPartitions.end()) {
        rawPartitions[outputRow] = idx;
        dataVector.setNoCopy(outputRow, anyValue);
        ++outputRow;
      }
    }
    return outputRow;
  }

  // A partition key is considered null if any of the partition key column in
  // 'input_' is null. The function records the rows has null partition key in
  // 'nullRows_'.
  void collectNullRows() {
    // Only calculate null rows when replicateNullsAndAny_ is to be applied
    if (!replicateNullsAndAny_ || numPartitions_ == 1) {
      nullRows_.clearAll();
      return;
    }

    auto size = input_->size();
    nullRows_.resize(size);
    nullRows_.clearAll();

    decodedVectors_.resize(keyChannels_.size());

    for (auto partitionKey : keyChannels_) {
      auto& keyVector = input_->childAt(partitionKey);
      if (keyVector->mayHaveNulls()) {
        decodedVectors_[partitionKey].decode(*keyVector);
        if (auto* rawNulls = decodedVectors_[partitionKey].nulls()) {
          bits::orWithNegatedBits(
              nullRows_.asMutableRange().bits(), rawNulls, 0, size);
        }
      }
    }
    nullRows_.updateBounds();
  }

  // The size of the output vector depends on the value of
  // 'replicateNullsAndAny_'. If replicateNullsAndAny_ is true, we need to
  // duplicate all input rows with null partition keys to ALL partition and also
  // duplicate a non-null value to each empty partition.
  // If there is no such row, then replicate any chosen input row to each empty
  // partition. It is ok to over-allocate and then resize to a lower value
  // instead of paying reallocation(s) cost.
  vector_size_t calculateMaxOutputSize() const {
    const vector_size_t numInput = input_->size();
    vector_size_t maxOutputSize = numInput;
    if (replicateNullsAndAny_ && numPartitions_ > 1) {
      const int32_t nullCount = nullRows_.countSelected();
      // TODO: If numPartitions_ and nullCount is large, we need to yield output
      // in batch.
      if (nullCount > 0) {
        maxOutputSize = numInput + (nullCount * numPartitions_);
      } else {
        maxOutputSize = numInput + numPartitions_;
      }
    }
    return maxOutputSize;
  }

  void computePartitions(FlatVector<int32_t>& partitionsVector) {
    auto numInput = input_->size();
    partitions_.resize(numInput);
    if (numPartitions_ == 1) {
      std::fill(partitions_.begin(), partitions_.end(), 0);
    } else {
      partitionFunction_->partition(*input_, partitions_);
    }

    // TODO Avoid copy.
    auto rawPartitions = partitionsVector.mutableRawValues();
    ::memcpy(rawPartitions, partitions_.data(), sizeof(int32_t) * numInput);
  }

  RowVectorPtr reorderInputsIfNeeded() {
    if (serializedColumnIndices_.empty()) {
      return input_;
    }

    const auto& inputColumns = input_->children();
    std::vector<VectorPtr> columns(inputColumns.size());
    for (auto i = 0; i < columns.size(); ++i) {
      columns[i] = inputColumns[serializedColumnIndices_[i]];
    }
    return std::make_shared<RowVector>(
        input_->pool(),
        serializedRowType_,
        nullptr,
        input_->size(),
        std::move(columns));
  }

  // The logic of this method is logically identical with
  // UnsafeRowVectorSerializer::append() and UnsafeRowVectorSerializer::flush().
  // Rewriting of the serialization logic here to avoid additional copies so
  // that contents are directly written into passed in vector.
  void serializeRows(FlatVector<StringView>& dataVector) {
    const auto numInput = input_->size();

    // Compute row sizes.
    rowSizes_.resize(numInput);

    velox::row::UnsafeRowFast unsafeRow(reorderInputsIfNeeded());

    size_t totalSize = 0;
    if (auto fixedRowSize = unsafeRow.fixedRowSize(asRowType(input_->type()))) {
      totalSize += fixedRowSize.value() * numInput;
      std::fill(rowSizes_.begin(), rowSizes_.end(), fixedRowSize.value());
    } else {
      for (auto i = 0; i < numInput; ++i) {
        const size_t rowSize = unsafeRow.rowSize(i);
        rowSizes_[i] = rowSize;
        totalSize += rowSize;
      }
    }

    // Allocate memory.
    auto buffer = dataVector.getBufferWithSpace(totalSize);
    // getBufferWithSpace() may return a buffer that already has content, so we
    // only use the space after that.
    auto rawBuffer = buffer->asMutable<char>() + buffer->size();
    buffer->setSize(buffer->size() + totalSize);
    memset(rawBuffer, 0, totalSize);

    // Serialize rows.
    size_t offset = 0;
    for (auto i = 0; i < numInput; ++i) {
      dataVector.setNoCopy(i, StringView(rawBuffer + offset, rowSizes_[i]));

      // Write row data.
      auto size = unsafeRow.serialize(i, rawBuffer + offset);
      VELOX_DCHECK_EQ(size, rowSizes_[i]);
      offset += size;
    }
  }

  const uint32_t numPartitions_;
  const RowTypePtr serializedRowType_;
  const std::vector<column_index_t> keyChannels_;
  const std::unique_ptr<core::PartitionFunction> partitionFunction_;
  bool replicateNullsAndAny_;
  std::vector<column_index_t> serializedColumnIndices_;

  // Decoded 'keyChannels_' columns.
  std::vector<velox::DecodedVector> decodedVectors_;
  // Identifies the input rows which has null partition keys.
  velox::SelectivityVector nullRows_;
  // Reusable vector for storing partition id for each input row.
  std::vector<uint32_t> partitions_;
  // Reusable vector for storing serialised row size for each input row.
  std::vector<uint32_t> rowSizes_;
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
  stream << ") " << numPartitions_ << " " << partitionFunctionSpec_->toString()
         << " " << serializedRowType_->toString();
}

folly::dynamic PartitionAndSerializeNode::serialize() const {
  auto obj = PlanNode::serialize();
  obj["keys"] = ISerializable::serialize(keys_);
  obj["numPartitions"] = numPartitions_;
  obj["serializedRowType"] = serializedRowType_->serialize();
  obj["sources"] = ISerializable::serialize(sources_);
  obj["replicateNullsAndAny"] = replicateNullsAndAny_;
  obj["partitionFunctionSpec"] = partitionFunctionSpec_->serialize();
  return obj;
}

velox::core::PlanNodePtr PartitionAndSerializeNode::create(
    const folly::dynamic& obj,
    void* context) {
  return std::make_shared<PartitionAndSerializeNode>(
      deserializePlanNodeId(obj),
      ISerializable::deserialize<std::vector<velox::core::ITypedExpr>>(
          obj["keys"], context),
      obj["numPartitions"].asInt(),
      ISerializable::deserialize<RowType>(obj["serializedRowType"], context),
      ISerializable::deserialize<std::vector<velox::core::PlanNode>>(
          obj["sources"], context)[0],
      obj["replicateNullsAndAny"].asBool(),
      ISerializable::deserialize<velox::core::PartitionFunctionSpec>(
          obj["partitionFunctionSpec"], context));
}
} // namespace facebook::presto::operators
