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
#pragma once

#include "velox/core/PlanNode.h"
#include "velox/exec/Operator.h"

namespace facebook::presto::operators {

/// Velox `PlanNode` wrapping `velox::connector::hive::iceberg::
/// IcebergMergeProcessor`. Single-input, single-output stateless transform
/// that fans MERGE / UPDATE input rows into delete + insert rows for the
/// `DELETE_ROW_AND_INSERT_ROW` row-change paradigm. Mirrors the OSS Java
/// `MergeProcessorOperator` (which wraps `DeleteAndInsertMergeProcessor`).
///
/// The node is what `PrestoToVeloxQueryPlan` produces when translating a
/// `protocol::MergeProcessorNode` for an iceberg target. The dispatch from
/// the protocol struct happens in `IcebergPrestoToVeloxMergeProcessor`
/// (Layer 3b).
///
/// Input contract (channels in this fixed order, set by the upstream
/// projection produced by the coordinator):
///   0. unique_id        BIGINT       (not consumed)
///   1. target_row_id    ROW          (Iceberg row id — copied verbatim
///                                     into the output on DELETE rows)
///   2. merge_row        ROW          (target column 0..N-1 + operation
///                                     TINYINT + case_number INTEGER)
///   3. case_number      INTEGER      (not consumed)
///   4. is_distinct      BOOLEAN      (not consumed)
///
/// The channel indices for target_row_id and merge_row are configurable
/// via `targetRowIdChannel` / `mergeRowChannel` to match the upstream
/// projection produced by the coordinator.
///
/// Output contract (set by IcebergMergeProcessor::outputType()):
///   columns 0..N-1     : target column values
///   column  N          : operation TINYINT — INSERT (1) or DELETE (2)
///   column  N+1        : rowId — same type as input row id; verbatim
///                        copy on DELETE, NULL on INSERT
///   column  N+2        : insert_from_update TINYINT — 1 if the INSERT
///                        half of an UPDATE, 0 otherwise
class IcebergMergeProcessorNode : public velox::core::PlanNode {
 public:
  IcebergMergeProcessorNode(
      const velox::core::PlanNodeId& id,
      std::vector<velox::TypePtr> targetColumnTypes,
      std::vector<std::string> outputColumnNames,
      velox::TypePtr rowIdType,
      velox::column_index_t targetRowIdChannel,
      velox::column_index_t mergeRowChannel,
      velox::core::PlanNodePtr source)
      : velox::core::PlanNode(id),
        targetColumnTypes_(std::move(targetColumnTypes)),
        outputColumnNames_(std::move(outputColumnNames)),
        rowIdType_(std::move(rowIdType)),
        targetRowIdChannel_(targetRowIdChannel),
        mergeRowChannel_(mergeRowChannel),
        sources_({std::move(source)}),
        outputType_(buildOutputType(
            targetColumnTypes_,
            outputColumnNames_,
            rowIdType_)) {
    VELOX_USER_CHECK_NOT_NULL(
        sources_[0], "IcebergMergeProcessorNode source cannot be null");
    VELOX_USER_CHECK_NOT_NULL(
        rowIdType_, "IcebergMergeProcessorNode rowIdType cannot be null");
    VELOX_USER_CHECK(
        !targetColumnTypes_.empty(),
        "IcebergMergeProcessorNode targetColumnTypes cannot be empty");
    VELOX_USER_CHECK_EQ(
        outputColumnNames_.size(),
        targetColumnTypes_.size() + 3,
        "IcebergMergeProcessorNode outputColumnNames size must equal "
        "targetColumnTypes.size() + 3 (target cols + operation + row_id + "
        "insert_from_update)");
  }

  /// Builder mirrors the `PartitionAndSerializeNode::Builder` shape.
  class Builder {
   public:
    Builder() = default;

    explicit Builder(const IcebergMergeProcessorNode& other) {
      id_ = other.id();
      targetColumnTypes_ = other.targetColumnTypes();
      outputColumnNames_ = other.outputColumnNames();
      rowIdType_ = other.rowIdType();
      targetRowIdChannel_ = other.targetRowIdChannel();
      mergeRowChannel_ = other.mergeRowChannel();
      source_ = other.sources()[0];
    }

    Builder& id(velox::core::PlanNodeId id) {
      id_ = std::move(id);
      return *this;
    }

    Builder& targetColumnTypes(std::vector<velox::TypePtr> targetColumnTypes) {
      targetColumnTypes_ = std::move(targetColumnTypes);
      return *this;
    }

    Builder& outputColumnNames(std::vector<std::string> outputColumnNames) {
      outputColumnNames_ = std::move(outputColumnNames);
      return *this;
    }

    Builder& rowIdType(velox::TypePtr rowIdType) {
      rowIdType_ = std::move(rowIdType);
      return *this;
    }

    Builder& targetRowIdChannel(velox::column_index_t targetRowIdChannel) {
      targetRowIdChannel_ = targetRowIdChannel;
      return *this;
    }

    Builder& mergeRowChannel(velox::column_index_t mergeRowChannel) {
      mergeRowChannel_ = mergeRowChannel;
      return *this;
    }

    Builder& source(velox::core::PlanNodePtr source) {
      source_ = std::move(source);
      return *this;
    }

    std::shared_ptr<IcebergMergeProcessorNode> build() const {
      VELOX_USER_CHECK(
          id_.has_value(), "IcebergMergeProcessorNode id is not set");
      VELOX_USER_CHECK(
          targetColumnTypes_.has_value(),
          "IcebergMergeProcessorNode targetColumnTypes is not set");
      VELOX_USER_CHECK(
          outputColumnNames_.has_value(),
          "IcebergMergeProcessorNode outputColumnNames is not set");
      VELOX_USER_CHECK(
          rowIdType_.has_value(),
          "IcebergMergeProcessorNode rowIdType is not set");
      VELOX_USER_CHECK(
          targetRowIdChannel_.has_value(),
          "IcebergMergeProcessorNode targetRowIdChannel is not set");
      VELOX_USER_CHECK(
          mergeRowChannel_.has_value(),
          "IcebergMergeProcessorNode mergeRowChannel is not set");
      VELOX_USER_CHECK(
          source_.has_value(), "IcebergMergeProcessorNode source is not set");
      return std::make_shared<IcebergMergeProcessorNode>(
          id_.value(),
          targetColumnTypes_.value(),
          outputColumnNames_.value(),
          rowIdType_.value(),
          targetRowIdChannel_.value(),
          mergeRowChannel_.value(),
          source_.value());
    }

   private:
    std::optional<velox::core::PlanNodeId> id_;
    std::optional<std::vector<velox::TypePtr>> targetColumnTypes_;
    std::optional<std::vector<std::string>> outputColumnNames_;
    std::optional<velox::TypePtr> rowIdType_;
    std::optional<velox::column_index_t> targetRowIdChannel_;
    std::optional<velox::column_index_t> mergeRowChannel_;
    std::optional<velox::core::PlanNodePtr> source_;
  };

  folly::dynamic serialize() const override;

  static velox::core::PlanNodePtr create(
      const folly::dynamic& obj,
      void* context);

  const velox::RowTypePtr& outputType() const override {
    return outputType_;
  }

  const std::vector<velox::core::PlanNodePtr>& sources() const override {
    return sources_;
  }

  std::string_view name() const override {
    return "IcebergMergeProcessor";
  }

  const std::vector<velox::TypePtr>& targetColumnTypes() const {
    return targetColumnTypes_;
  }

  const std::vector<std::string>& outputColumnNames() const {
    return outputColumnNames_;
  }

  const velox::TypePtr& rowIdType() const {
    return rowIdType_;
  }

  velox::column_index_t targetRowIdChannel() const {
    return targetRowIdChannel_;
  }

  velox::column_index_t mergeRowChannel() const {
    return mergeRowChannel_;
  }

 private:
  void addDetails(std::stringstream& stream) const override;

  /// Builds the output RowType once at construction time. Format matches
  /// IcebergMergeProcessor::outputType() exactly:
  ///   targetColumnTypes_ ... ++ [TINYINT, rowIdType_, TINYINT].
  /// All output column names — including the trailing operation, rowId, and
  /// insertFromUpdate columns — come from `outputColumnNames` (size N+3) so
  /// the writer's name-based binding (TableWriter::setTypeMappings) sees
  /// iceberg/planner-correct names rather than synthetic positional ones.
  static velox::RowTypePtr buildOutputType(
      const std::vector<velox::TypePtr>& targetColumnTypes,
      const std::vector<std::string>& outputColumnNames,
      const velox::TypePtr& rowIdType);

  const std::vector<velox::TypePtr> targetColumnTypes_;
  const std::vector<std::string> outputColumnNames_;
  const velox::TypePtr rowIdType_;
  const velox::column_index_t targetRowIdChannel_;
  const velox::column_index_t mergeRowChannel_;
  const std::vector<velox::core::PlanNodePtr> sources_;
  const velox::RowTypePtr outputType_;
};

/// Plan-node translator that Velox's exec engine consults to construct the
/// `IcebergMergeProcessorOperator` from an `IcebergMergeProcessorNode`.
/// Register once at server startup via `PrestoServer::registerCustomOperators`.
class IcebergMergeProcessorTranslator
    : public velox::exec::Operator::PlanNodeTranslator {
 public:
  std::unique_ptr<velox::exec::Operator> toOperator(
      velox::exec::DriverCtx* ctx,
      int32_t id,
      const velox::core::PlanNodePtr& node) override;
};

} // namespace facebook::presto::operators
