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
#include "velox/row/UnsafeRowFast.h"

using namespace facebook::velox::exec;
using namespace facebook::velox;

namespace facebook::presto::operators {
namespace {
velox::core::PlanNodeId deserializePlanNodeId(const folly::dynamic& obj) {
  return obj["id"].asString();
}

/// Various state of execution of a PartitionAndSerializeOperator instance.
enum class ProcessingState {
  //  Unknown state and can only be valid during
  // initialization chain or as an default value. If we get to this state while
  // execution, we likely throw an
  // error.
  kUnknown,
  // Process only the input elements without
  // consideration to extraneous states like replicateNullAndAny. This state is
  // visited once for each input vector.
  kProcessingInputElements,
  // Replicate null values until we have filled up the
  // outputVector_. This state is visited multiple times until we have
  // replicated all null values.
  kProcessingNulls,
  // Replicate 'any' value. This state is visited once in this
  // operator's lifecycle (at the end), when all the input batches have been
  // processed.
  kProcessingAny,
  //  We have processed the input vector, and can start consuming the
  // next batch of input.
  kDone,
};

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
        replicateNullsAndAny_(
            numPartitions_ > 1 ? planNode->isReplicateNullsAndAny() : false),
        partitionFunction_(
            numPartitions_ == 1 ? nullptr
                                : planNode->partitionFunctionFactory()->create(
                                      planNode->numPartitions())),
        serializedRowType_{planNode->serializedRowType()},
        keyChannels_(toChannels(
            planNode->sources()[0]->outputType(),
            planNode->keys())) {
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
    numInput_ = input_->size();
    if (replicateNullsAndAny_) {
      collectNullRows();
    }
    // We will be processing the input elements before any other.
    currentState_ = ProcessingState::kProcessingInputElements;
    nextPartitionId_ = 0;

    velox::row::UnsafeRowFast unsafeRow(reorderInputsIfNeeded());
    calculateSerializedRowSize(unsafeRow);

    outputVector_ = createOutputRowVector(numInput_);
    auto partitionVector = outputVector_->childAt(0)->asFlatVector<int32_t>();
    auto dataVector = outputVector_->childAt(1)->asFlatVector<StringView>();

    computePartitions(*partitionVector);
    serializeRows(unsafeRow, *dataVector);
    numOutputRows_ = numInput_;
    input_ = nullptr;
    if (replicateNullsAndAny_) {
      if (numInputNulls_ > 0) {
        // Since there are null values, set the desired output vector size for
        // when processing null values in kProcessingNulls stage.
        decideOutputSize();
      } else {
        // Capture the first element of the current input vector, this is used
        // to replicate to all partitions in kProcessingAny stage.
        replicateAnyOriginalData_ = dataVector->valueAt(0);
        replicateAnyOriginalPartition_ = partitionVector->valueAt(0);
      }
    }
  }

  // PartitionAndSerialize is a state machine, the state is maintained in
  // 'currentState_'. 'getOutput' will generate the outputVector_ based on
  // 'currentState_' and set member variables accordingly.
  RowVectorPtr getOutput() override {
    if (input_ == nullptr && currentState_ == ProcessingState::kUnknown) {
      // Initialization chain, not ready to process any input.
      return nullptr;
    }
    switch (currentState_) {
      case ProcessingState::kProcessingInputElements:
        // We don't need to do anything here since all the work is already done
        // in 'addInput'
        break;
      case ProcessingState::kProcessingNulls: {
        // Begin writing from start index of outputVector_.
        numOutputRows_ = 0;
        outputVector_ = createOutputRowVector(desiredOutputSize_);
        // Keep processing until we have filled up the output vector OR we are
        // done with all partitions.
        while (numOutputRows_ + numInputNulls_ <= desiredOutputSize_ &&
               nextPartitionId_ < numPartitions_) {
          numOutputRows_ = replicateRowsWithNullPartitionKeys(
              nextPartitionId_, numOutputRows_);
          ++nextPartitionId_;
        }
        break;
      }
      case ProcessingState::kProcessingAny: {
        outputVector_ = createOutputRowVector(numPartitions_);
        numOutputRows_ = replicateAnyToEmptyPartitions();
        break;
      }
      case ProcessingState::kDone:
        return nullptr;
      case ProcessingState::kUnknown:
        VELOX_UNREACHABLE("Undefined state reached, not sure what to do.")
    }
    currentState_ = getNextState();
    if (numOutputRows_ != 0) {
      resizeOutputVector(outputVector_, numOutputRows_);
      return outputVector_;
    }
    return nullptr;
  }

  BlockingReason isBlocked(ContinueFuture* future) override {
    return BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return noMoreInput_;
  }

  // The size of the output vector depends on the state we are processing. This
  // is only used when we are processing 'kProcessingNulls' state since the
  // output vector can be huge based on 'numInputNulls_' and 'numPartitions_'
  void decideOutputSize() {
    // cumulativeRowSizes_ does not include the size of the last element.
    auto averageRowSize = (cumulativeRowSizes_.back() + rowSizes_.back()) /
        cumulativeRowSizes_.size();
    auto desiredOutputBatchRows = Operator::outputBatchRows(averageRowSize);
    if (desiredOutputBatchRows < numInput_) {
      desiredOutputBatchRows = numInput_;
    }
    desiredOutputSize_ =
        (desiredOutputBatchRows / numInputNulls_ + 1) * numInputNulls_;
  }

 private:
  // The 'getNextState' function is responsible for determining the next state
  // based on the current state and various factors.
  ProcessingState getNextState() const {
    switch (currentState_) {
      case ProcessingState::kUnknown:
        // In an unknown state, the next state is also unknown.
        return ProcessingState::kUnknown;
      case ProcessingState::kProcessingInputElements: {
        if (!replicateNullsAndAny_)
          // If replicateNullsAndAny_ is false, the processing is complete, and
          // the next state is the "done" state.
          return ProcessingState::kDone;
        else if (numInputNulls_ > 0)
          // If there are null elements (numInputNulls_ > 0), the next state is
          // the "processing nulls" state.
          return ProcessingState::kProcessingNulls;
        else if (anyNullValue)
          // If there are no null elements in the current batch but null
          // elements existed in the past (anyNullValue == true), the next state
          // is the "done" state, as no further replication is required (we
          // don't need to 'replicateAny' as none of the partitions can possibly
          // be empty).
          return ProcessingState::kDone;
        else
          // If there has been no null values in this batch or in the past. The
          // next state is the "processing any" state.
          return ProcessingState::kProcessingAny;
      }
      case ProcessingState::kProcessingNulls: {
        // If the next partition ID is less than the total number of partitions,
        // the next state remains in the "processing nulls" state.
        if (nextPartitionId_ < numPartitions_)
          return ProcessingState::kProcessingNulls;
        // Otherwise, the next state is the "done" state.
        return ProcessingState::kDone;
      }
      case ProcessingState::kProcessingAny:
        // The next state is always the "done" state, as processing any elements
        // does not require further replication.
        return ProcessingState::kDone;
      default:
        VELOX_UNREACHABLE("Invalid state");
    }
  }

  // Create output vector and resize all it's children appropriately.
  RowVectorPtr createOutputRowVector(vector_size_t size) {
    auto output = BaseVector::create<RowVector>(outputType_, size, pool());
    for (auto& child : output->children()) {
      child->resize(size);
    }

    if (serializedDataBuffer_ != nullptr) {
      auto dataVector = output->childAt(1)->asFlatVector<StringView>();
      // Hold the reference to serialized string buffer.
      dataVector->addStringBuffer(serializedDataBuffer_);
    }
    return output;
  }

  // Resize output vector and all of it's children.
  void resizeOutputVector(const RowVectorPtr& output, vector_size_t size) {
    output->resize(size);
    for (auto& child : output->children()) {
      child->resize(size);
    }
  }

  // Replicate input rows with null partition keys to partitions
  // [partitionId, endPartitionId). Return the total number of output
  // rows after the replication.
  vector_size_t replicateRowsWithNullPartitionKeys(
      vector_size_t partitionId,
      vector_size_t nextRow) {
    auto partitionVector = outputVector_->childAt(0)->asFlatVector<int32_t>();
    auto dataVector = outputVector_->childAt(1)->asFlatVector<StringView>();

    auto rawBuffer =
        serializedDataBuffer_->as<char>() + serializedDataVectorOffset_;

    for (auto partition = partitionId; partition < partitionId + 1;
         ++partition) {
      for (auto row = 0; row < numInput_; ++row) {
        // Replicate a row with null partition key to all the other partitions.
        if (nullRows_.isValid(row) && partitions_[row] != partition) {
          partitionVector->set(nextRow, partition);
          dataVector->setNoCopy(
              nextRow,
              StringView(rawBuffer + cumulativeRowSizes_[row], rowSizes_[row]));
          ++nextRow;
        }
      }
    }
    return nextRow;
  }

  // Replicates the first value to all the empty partitions.
  vector_size_t replicateAnyToEmptyPartitions() {
    auto partitionVector = outputVector_->childAt(0)->asFlatVector<int32_t>();
    auto dataVector = outputVector_->childAt(1)->asFlatVector<StringView>();

    auto* rawPartitions = partitionVector->mutableRawValues();

    vector_size_t index = 0;
    for (auto i = 0; i < numPartitions_; ++i) {
      if (i != replicateAnyOriginalPartition_) {
        rawPartitions[index] = i;
        dataVector->setNoCopy(index, replicateAnyOriginalData_);
        ++index;
      }
    }
    return index;
  }

  // A partition key is considered null if any of the partition key column in
  // 'input_' is null. The function records the rows has null partition key in
  // 'nullRows_'.
  void collectNullRows() {
    nullRows_.resize(numInput_);
    nullRows_.clearAll();

    // TODO Avoid decoding keys twice: here and when computing partitions.
    decodedVectors_.resize(keyChannels_.size());

    for (auto partitionKey : keyChannels_) {
      auto& keyVector = input_->childAt(partitionKey);
      if (keyVector->mayHaveNulls()) {
        auto& decoded = decodedVectors_[partitionKey];
        decoded.decode(*keyVector);
        if (auto* rawNulls = decoded.nulls()) {
          bits::orWithNegatedBits(
              nullRows_.asMutableRange().bits(), rawNulls, 0, numInput_);
        }
      }
    }
    nullRows_.updateBounds();
    numInputNulls_ = nullRows_.countSelected();
    anyNullValue |= numInputNulls_;
  }

  void computePartitions(FlatVector<int32_t>& partitionsVector) {
    partitions_.resize(numInput_);
    if (numPartitions_ == 1) {
      std::fill(partitions_.begin(), partitions_.end(), 0);
    } else {
      partitionFunction_->partition(*input_, partitions_);
    }

    // TODO Avoid copy.
    auto rawPartitions = partitionsVector.mutableRawValues();
    ::memcpy(rawPartitions, partitions_.data(), sizeof(int32_t) * numInput_);
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

  // Calculate the size of each serialized row
  void calculateSerializedRowSize(velox::row::UnsafeRowFast& unsafeRow) {
    // Compute row sizes.
    rowSizes_.resize(numInput_);
    cumulativeRowSizes_.resize(numInput_);

    size_t totalSize = 0;
    if (auto fixedRowSize = unsafeRow.fixedRowSize(asRowType(input_->type()))) {
      totalSize += fixedRowSize.value() * numInput_;
      std::fill(rowSizes_.begin(), rowSizes_.end(), fixedRowSize.value());
      // Calculate the cumulative sum starting from index 0 and finish at n-1
      // cumulativeRowSizes_ should start with value 0 at index 0.
      std::partial_sum(
          rowSizes_.begin(),
          rowSizes_.end() - 1,
          cumulativeRowSizes_.begin() + 1);
    } else {
      for (auto i = 0; i < numInput_; ++i) {
        const size_t rowSize = unsafeRow.rowSize(i);
        rowSizes_[i] = rowSize;
        cumulativeRowSizes_[i] = totalSize;
        totalSize += rowSize;
      }
    }
  }

  // The logic of this method is logically identical with
  // UnsafeRowVectorSerializer::append() and
  // UnsafeRowVectorSerializer::flush(). Rewriting of the serialization logic
  // here to avoid additional copies so that contents are directly written
  // into passed in vector.
  void serializeRows(
      velox::row::UnsafeRowFast& unsafeRow,
      FlatVector<StringView>& dataVector) {
    size_t totalSize = cumulativeRowSizes_[cumulativeRowSizes_.size() - 1] +
        rowSizes_[rowSizes_.size() - 1];

    // Allocate memory.
    serializedDataBuffer_ = dataVector.getBufferWithSpace(totalSize);
    // getBufferWithSpace() may return a buffer that already has content, so we
    // only use the space after that.
    serializedDataVectorOffset_ = serializedDataBuffer_->size();
    auto rawBuffer =
        serializedDataBuffer_->asMutable<char>() + serializedDataVectorOffset_;
    serializedDataBuffer_->setSize(serializedDataBuffer_->size() + totalSize);
    memset(rawBuffer, 0, totalSize);

    // Serialize rows.
    size_t offset = 0;
    for (auto i = 0; i < numInput_; ++i) {
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
  const bool replicateNullsAndAny_;
  std::vector<column_index_t> serializedColumnIndices_;

  ProcessingState currentState_{ProcessingState::kUnknown};
  vector_size_t nextPartitionId_{0};

  // Input element which will be used for 'replicateAny'.
  StringView replicateAnyOriginalData_;
  // Original partition id of 'replicateAnyOriginalData_'.
  uint32_t replicateAnyOriginalPartition_;

  // Flag to indicate if we saw any null value in input vector so far.
  bool anyNullValue{false};
  // Count of null values in this batch of input.
  int32_t numInputNulls_{0};
  // Number of output rows in the output vector.
  vector_size_t numOutputRows_{0};
  // Size of the input vector being processed by this batch.
  vector_size_t numInput_{0};
  RowVectorPtr outputVector_;
  // Based on Operator::outputBatchRows, we try as close to the configured value
  // as possible.
  vector_size_t desiredOutputSize_;

  // Decoded 'keyChannels_' columns.
  std::vector<velox::DecodedVector> decodedVectors_;
  // Identifies the input rows which has null partition keys.
  velox::SelectivityVector nullRows_;
  // Reusable vector for storing partition id for each input row.
  std::vector<uint32_t> partitions_;
  // Reusable vector for storing serialized row size for each input row.
  std::vector<uint32_t> rowSizes_;

  // Keep reference of serialized data vector, this is used when processing
  // subsequent batches.
  BufferPtr serializedDataBuffer_;
  // Offset storing the starting index of serialized data in 'rawBuffer'.
  size_t serializedDataVectorOffset_;
  // Reusable vector for storing cumulative row size for each input row.
  std::vector<uint32_t> cumulativeRowSizes_;
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
