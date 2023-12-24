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
#include "velox/row/CompactRow.h"

using namespace facebook::velox::exec;
using namespace facebook::velox;

namespace facebook::presto::operators {
namespace {
velox::core::PlanNodeId deserializePlanNodeId(const folly::dynamic& obj) {
  return obj["id"].asString();
}

// The output of this operator has 2 columns:
// (1) partition number (INTEGER);
// (2) serialized row (VARBINARY)
//
// When replicateNullsAndAny is true, there is an extra boolean column that
// indicated whether a row should be replicated to all partitions.
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
        replicateNullsAndAny_(
            numPartitions_ > 1 ? planNode->isReplicateNullsAndAny() : false) {
    const auto& inputType = planNode->sources()[0]->outputType()->asRow();
    const auto& serializedRowTypeNames = serializedRowType_->names();
    bool identityMapping = (serializedRowType_->size() == inputType.size());
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
    const auto numInput = input_->size();

    // Reset state variables.
    nextOutputRow_ = 0;
    rowSizes_.clear();
    rowSizes_.resize(numInput);

    compactRow_ =
        std::make_unique<velox::row::CompactRow>(reorderInputsIfNeeded());
    calculateRowSize();

    // Process partitionVector and replicateVector once, and reuse on subsequent
    // batch.
    prepareOutput();
    auto* partitionVector = output_->childAt(0)->asFlatVector<int32_t>();
    computePartitions(*partitionVector);
    if (replicateNullsAndAny_) {
      auto* replicateVector = output_->childAt(1)->asFlatVector<bool>();
      populateReplicateFlags(*replicateVector);
    }
  }

  RowVectorPtr getOutput() override {
    if (!input_) {
      return nullptr;
    }
    vector_size_t endOutputRow;
    uint32_t outputBufferSize{0};
    prepareNextOutput(endOutputRow, outputBufferSize);
    VELOX_CHECK_LT(nextOutputRow_, endOutputRow);

    const auto batchSize = endOutputRow - nextOutputRow_;
    auto dataVector = BaseVector::create<FlatVector<StringView>>(
        VARBINARY(), batchSize, pool());
    serializeRows(*dataVector, outputBufferSize, nextOutputRow_, endOutputRow);

    // Extract slice from output_ and construct the output vector.
    std::vector<VectorPtr> childrenVectors;
    childrenVectors.push_back(
        output_->childAt(0)->slice(nextOutputRow_, batchSize));
    childrenVectors.push_back(dataVector);
    RowVectorPtr outputBatch;
    // Handle replicateVector based on 'replicateNullsAndAny_' as it
    // is optional.
    if (replicateNullsAndAny_) {
      childrenVectors.push_back(
          output_->childAt(1)->slice(nextOutputRow_, batchSize));
      outputBatch = std::make_shared<RowVector>(
          pool(),
          outputType_,
          nullptr /*nulls*/,
          batchSize,
          std::move(childrenVectors));
    } else {
      outputBatch = std::make_shared<RowVector>(
          pool(),
          ROW({INTEGER(), VARBINARY()}),
          nullptr /*nulls*/,
          batchSize,
          std::move(childrenVectors));
    }

    nextOutputRow_ = endOutputRow;
    if (nextOutputRow_ == input_->size()) {
      input_ = nullptr;
    }
    return outputBatch;
  }

  BlockingReason isBlocked(ContinueFuture* future) override {
    return BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return noMoreInput_;
  }

 private:
  void prepareOutput() {
    const auto size = input_->size();
    // Try to re-use memory for the output vectors that contain partitionVector
    // and replicateVector.
    if (output_) {
      VectorPtr output = std::move(output_);
      BaseVector::prepareForReuse(output, size);
      output_ = std::static_pointer_cast<RowVector>(output);
    } else {
      output_ = BaseVector::create<RowVector>(
          ROW({INTEGER(), BOOLEAN()}), size, pool());
    }
  }

  // Invoked to calculate how many rows to output: ['nextOutputRow_',
  // 'endOutputRow_') and the corresponding output buffer size returns in
  // 'outputBufferSize'.
  size_t prepareNextOutput(
      vector_size_t& endOutputRow,
      uint32_t& outputBufferSize) {
    const auto& queryConfig = operatorCtx_->driverCtx()->queryConfig();
    const auto preferredOutputBytes = queryConfig.preferredOutputBatchBytes();
    const auto preferredOutputRows = queryConfig.preferredOutputBatchRows();
    endOutputRow = nextOutputRow_;

    VELOX_DCHECK(!rowSizes_.empty(), "rowSizes_ can not be empty");
    do {
      outputBufferSize += rowSizes_[endOutputRow++];
    } while (endOutputRow < input_->size() &&
             outputBufferSize < preferredOutputBytes &&
             (endOutputRow - nextOutputRow_) < preferredOutputRows);

    return outputBufferSize;
  }

  // Calculates the size of each row. This is done once per input and reused for
  // each batch.
  void calculateRowSize() {
    if (auto fixedRowSize =
            compactRow_->fixedRowSize(asRowType(serializedRowType_))) {
      std::fill(rowSizes_.begin(), rowSizes_.end(), fixedRowSize.value());
    } else {
      const auto numInput = input_->size();
      for (auto i = 0; i < numInput; ++i) {
        const size_t rowSize = compactRow_->rowSize(i);
        rowSizes_[i] = rowSize;
      }
    }
  }

  // Populate the replicate flags for each row. This is done once per input and
  // reused for each batch.
  void populateReplicateFlags(FlatVector<bool>& replicateVector) {
    const auto numInput = input_->size();
    replicateVector.resize(numInput);
    auto* rawReplicatedValues = replicateVector.mutableRawValues<uint64_t>();
    memset(rawReplicatedValues, 0, bits::nbytes(numInput));

    decodedVectors_.resize(keyChannels_.size());

    for (auto partitionKey : keyChannels_) {
      if (partitionKey == kConstantChannel) {
        continue;
      }
      auto& keyVector = input_->childAt(partitionKey);
      if (keyVector->mayHaveNulls()) {
        decodedVectors_[partitionKey].decode(*keyVector);
        if (auto* rawNulls = decodedVectors_[partitionKey].nulls()) {
          bits::orWithNegatedBits(rawReplicatedValues, rawNulls, 0, numInput);
        }
      }
    }

    if (!replicatedAny_) {
      if (bits::countBits(rawReplicatedValues, 0, numInput) == 0) {
        replicateVector.set(0, true);
      }
      replicatedAny_ = true;
    }
  }

  // Computes the partition id for each row. This is done once per input and
  // reused for each batch.
  void computePartitions(FlatVector<int32_t>& partitionsVector) {
    auto numInput = input_->size();
    partitions_.resize(numInput);
    partitionsVector.resize(numInput);

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
    std::vector<VectorPtr> columns(serializedColumnIndices_.size());
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
  // UnsafeRowVectorSerializer::append() and
  // UnsafeRowVectorSerializer::flush(). Rewriting of the serialization logic
  // here to avoid additional copies so that contents are directly written into
  // passed in vector.
  // Since serialization can be memory intensive, we process only one batch at
  // a time.
  void serializeRows(
      FlatVector<StringView>& dataVector,
      size_t outputBufferSize,
      vector_size_t from,
      vector_size_t to) {
    vector_size_t batchSize = to - from;
    // Allocate memory.
    auto buffer = dataVector.getBufferWithSpace(outputBufferSize);

    // getBufferWithSpace() may return a buffer that already has content, so we
    // only use the space after that.
    auto rawBuffer = buffer->asMutable<char>() + buffer->size();
    buffer->setSize(buffer->size() + outputBufferSize);
    memset(rawBuffer, 0, outputBufferSize);

    // Serialize rows.
    size_t offset = 0;
    for (auto i = 0; i < batchSize; ++i) {
      // Write row data.
      auto size = compactRow_->serialize(from + i, rawBuffer + offset);
      VELOX_DCHECK_EQ(size, rowSizes_[from + i]);

      dataVector.setNoCopy(
          i, StringView(rawBuffer + offset, rowSizes_[from + i]));
      offset += size;
    }

    {
      auto lockedStats = stats_.wlock();
      lockedStats->addRuntimeStat(
          "serializedBytes", RuntimeCounter(outputBufferSize));
    }
  }

  const uint32_t numPartitions_;
  const RowTypePtr serializedRowType_;
  const std::vector<column_index_t> keyChannels_;
  const std::unique_ptr<core::PartitionFunction> partitionFunction_;
  const bool replicateNullsAndAny_;
  bool replicatedAny_{false};
  std::vector<column_index_t> serializedColumnIndices_;
  // Holder for partitionVector and replicateVector.
  RowVectorPtr output_;

  std::unique_ptr<velox::row::CompactRow> compactRow_;
  // Decoded 'keyChannels_' columns.
  std::vector<velox::DecodedVector> decodedVectors_;
  // Reusable vector for storing partition id for each input row.
  std::vector<uint32_t> partitions_;
  // Reusable vector for storing serialised row size for each input row.
  std::vector<uint32_t> rowSizes_;
  vector_size_t nextOutputRow_{0};
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
