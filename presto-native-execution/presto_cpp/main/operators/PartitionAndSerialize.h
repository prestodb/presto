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

/// Partitions the input row based on partition function and serializes the
/// entire row using UnsafeRow format. The output contains 3 columns: partition
/// number (INTEGER) serialized key (VARBINARY), and serialized row (VARBINARY).
/// If 'replicateNullsAndAny' is true, the output includes a third boolean
/// column which indicates whether a row needs to be replicated to all
/// partitions.
class PartitionAndSerializeNode : public velox::core::PlanNode {
 public:
  PartitionAndSerializeNode(
      const velox::core::PlanNodeId& id,
      std::vector<velox::core::TypedExprPtr> keys,
      uint32_t numPartitions,
      velox::RowTypePtr serializedRowType,
      velox::core::PlanNodePtr source,
      bool replicateNullsAndAny,
      velox::core::PartitionFunctionSpecPtr partitionFunctionFactory,
      std::optional<std::vector<velox::core::SortOrder>> sortingOrders =
          std::nullopt,
      std::optional<std::vector<velox::core::FieldAccessTypedExprPtr>>
          sortingKeys = std::nullopt)
      : velox::core::PlanNode(id),
        keys_(std::move(keys)),
        numPartitions_(numPartitions),
        serializedRowType_{std::move(serializedRowType)},
        sources_({std::move(source)}),
        replicateNullsAndAny_(replicateNullsAndAny && numPartitions > 1),
        partitionFunctionSpec_(std::move(partitionFunctionFactory)),
        sortingOrders_(std::move(sortingOrders)),
        sortingKeys_(std::move(sortingKeys)) {
    VELOX_USER_CHECK_NOT_NULL(
        partitionFunctionSpec_, "Partition function factory cannot be null.");
  }

  class Builder {
   public:
    Builder() = default;

    explicit Builder(const PartitionAndSerializeNode& other) {
      id_ = other.id();
      keys_ = other.keys();
      numPartitions_ = other.numPartitions();
      serializedRowType_ = other.serializedRowType();
      source_ = other.sources()[0];
      replicateNullsAndAny_ = other.isReplicateNullsAndAny();
      partitionFunctionFactory_ = other.partitionFunctionFactory();
      sortingOrders_ = other.sortingOrders();
      sortingKeys_ = other.sortingKeys();
    }

    Builder& id(velox::core::PlanNodeId id) {
      id_ = std::move(id);
      return *this;
    }

    Builder& keys(std::vector<velox::core::TypedExprPtr> keys) {
      keys_ = std::move(keys);
      return *this;
    }

    Builder& numPartitions(uint32_t numPartitions) {
      numPartitions_ = numPartitions;
      return *this;
    }

    Builder& serializedRowType(velox::RowTypePtr serializedRowType) {
      serializedRowType_ = std::move(serializedRowType);
      return *this;
    }

    Builder& source(velox::core::PlanNodePtr source) {
      source_ = std::move(source);
      return *this;
    }

    Builder& replicateNullsAndAny(bool replicateNullsAndAny) {
      replicateNullsAndAny_ = replicateNullsAndAny;
      return *this;
    }

    Builder& partitionFunctionFactory(
        velox::core::PartitionFunctionSpecPtr partitionFunctionFactory) {
      partitionFunctionFactory_ = std::move(partitionFunctionFactory);
      return *this;
    }

    Builder& sortingOrders(
        std::optional<std::vector<velox::core::SortOrder>> sortingOrders) {
      sortingOrders_ = std::move(sortingOrders);
      return *this;
    }

    Builder& sortingKeys(
        std::optional<std::vector<velox::core::FieldAccessTypedExprPtr>>
            sortingKeys) {
      sortingKeys_ = sortingKeys;
      return *this;
    }

    std::shared_ptr<PartitionAndSerializeNode> build() const {
      VELOX_USER_CHECK(
          id_.has_value(), "PartitionAndSerializeNode id is not set");
      VELOX_USER_CHECK(
          keys_.has_value(), "PartitionAndSerializeNode keys is not set");
      VELOX_USER_CHECK(
          numPartitions_.has_value(),
          "PartitionAndSerializeNode numPartitions is not set");
      VELOX_USER_CHECK(
          serializedRowType_.has_value(),
          "PartitionAndSerializeNode serializedRowType is not set");
      VELOX_USER_CHECK(
          source_.has_value(), "PartitionAndSerializeNode source is not set");
      VELOX_USER_CHECK(
          replicateNullsAndAny_.has_value(),
          "PartitionAndSerializeNode replicateNullsAndAny is not set");
      VELOX_USER_CHECK(
          partitionFunctionFactory_.has_value(),
          "PartitionAndSerializeNode partitionFunctionFactory is not set");

      return std::make_shared<PartitionAndSerializeNode>(
          id_.value(),
          keys_.value(),
          numPartitions_.value(),
          serializedRowType_.value(),
          source_.value(),
          replicateNullsAndAny_.value(),
          partitionFunctionFactory_.value(),
          sortingOrders_,
          sortingKeys_);
    }

   private:
    std::optional<velox::core::PlanNodeId> id_;
    std::optional<std::vector<velox::core::TypedExprPtr>> keys_;
    std::optional<uint32_t> numPartitions_;
    std::optional<velox::RowTypePtr> serializedRowType_;
    std::optional<velox::core::PlanNodePtr> source_;
    std::optional<bool> replicateNullsAndAny_;
    std::optional<velox::core::PartitionFunctionSpecPtr>
        partitionFunctionFactory_;
    std::optional<std::vector<velox::core::SortOrder>> sortingOrders_;
    std::optional<std::vector<velox::core::FieldAccessTypedExprPtr>>
        sortingKeys_;
  };

  folly::dynamic serialize() const override;

  static velox::core::PlanNodePtr create(
      const folly::dynamic& obj,
      void* context);

  const velox::RowTypePtr& outputType() const override {
    static const velox::RowTypePtr kOutputType{velox::ROW(
        {"partition", "key", "data"},
        {velox::INTEGER(), velox::VARBINARY(), velox::VARBINARY()})};

    static const velox::RowTypePtr kReplicateNullsAndAnyOutputType{velox::ROW(
        {"partition", "key", "data", "replicate"},
        {velox::INTEGER(),
         velox::VARBINARY(),
         velox::VARBINARY(),
         velox::BOOLEAN()})};

    return replicateNullsAndAny_ ? kReplicateNullsAndAnyOutputType
                                 : kOutputType;
  }

  const std::vector<velox::core::PlanNodePtr>& sources() const override {
    return sources_;
  }

  const std::vector<velox::core::TypedExprPtr>& keys() const {
    return keys_;
  }

  uint32_t numPartitions() const {
    return numPartitions_;
  }

  const velox::RowTypePtr& serializedRowType() const {
    return serializedRowType_;
  }

  /// Returns true if an arbitrary row and all rows with null keys must be
  /// replicated to all destinations. This is used to ensure correct results for
  /// anti-join which requires all nodes to know whether combined build side is
  /// empty and whether it has any entry with null join key.
  bool isReplicateNullsAndAny() const {
    return replicateNullsAndAny_;
  }

  const velox::core::PartitionFunctionSpecPtr& partitionFunctionFactory()
      const {
    return partitionFunctionSpec_;
  }

  std::string_view name() const override {
    return "PartitionAndSerialize";
  }

  const std::optional<std::vector<velox::core::FieldAccessTypedExprPtr>>&
  sortingKeys() const {
    return sortingKeys_;
  }

  const std::optional<std::vector<velox::core::SortOrder>>& sortingOrders()
      const {
    return sortingOrders_;
  }

 private:
  void addDetails(std::stringstream& stream) const override;

  const std::vector<velox::core::TypedExprPtr> keys_;
  const uint32_t numPartitions_;
  const velox::RowTypePtr serializedRowType_;
  const std::vector<velox::core::PlanNodePtr> sources_;
  const bool replicateNullsAndAny_;
  const velox::core::PartitionFunctionSpecPtr partitionFunctionSpec_;
  const std::optional<std::vector<velox::core::SortOrder>> sortingOrders_;
  const std::optional<std::vector<velox::core::FieldAccessTypedExprPtr>>
      sortingKeys_;
};

class PartitionAndSerializeTranslator
    : public velox::exec::Operator::PlanNodeTranslator {
 public:
  std::unique_ptr<velox::exec::Operator> toOperator(
      velox::exec::DriverCtx* ctx,
      int32_t id,
      const velox::core::PlanNodePtr& node) override;
};
} // namespace facebook::presto::operators
