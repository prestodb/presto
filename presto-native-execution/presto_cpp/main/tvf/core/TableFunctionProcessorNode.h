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

#include "presto_cpp/main/tvf/spi/TableFunction.h"

#include "velox/core/Expressions.h"
#include "velox/core/PlanNode.h"

namespace facebook::presto::tvf {

class TableFunctionProcessorNode : public velox::core::PlanNode {
 public:
  struct PassThroughColumnSpecification {
   public:
    PassThroughColumnSpecification(
        bool isPartitioningColumn,
        velox::column_index_t inputChannel,
        velox::column_index_t outputChannel,
        velox::column_index_t indexChannel)
        : isPartitioningColumn_(isPartitioningColumn),
          inputChannel_(inputChannel),
          outputChannel_(outputChannel),
          indexChannel_(indexChannel) {}

    velox::column_index_t inputChannel() const {
      return inputChannel_;
    }

    velox::column_index_t outputChannel() const {
      return outputChannel_;
    }

    velox::column_index_t indexChannel() const {
      return indexChannel_;
    }

    bool isPartitioningColumn() const {
      return isPartitioningColumn_;
    }

    folly::dynamic serialize() const {
      folly::dynamic obj = folly::dynamic::object;
      obj["isPartitioningColumn"] = isPartitioningColumn_;
      obj["inputChannel"] = inputChannel_;
      obj["outputChannel"] = outputChannel_;
      obj["indexChannel"] = indexChannel_;
      return obj;
    }

    static PassThroughColumnSpecification deserialize(
        const folly::dynamic& obj) {
      return {
          obj["isPartitioningColumn"].asBool(),
          static_cast<velox::column_index_t>(obj["inputChannel"].asInt()),
          static_cast<velox::column_index_t>(obj["outputChannel"].asInt()),
          static_cast<velox::column_index_t>(obj["indexChannel"].asInt())};
    }

   private:
    const bool isPartitioningColumn_;
    const velox::column_index_t inputChannel_;
    const velox::column_index_t outputChannel_;
    const velox::column_index_t indexChannel_;
  };

  TableFunctionProcessorNode(
      velox::core::PlanNodeId id,
      std::string name,
      TableFunctionHandlePtr handle,
      std::vector<velox::core::FieldAccessTypedExprPtr> partitionKeys,
      std::vector<velox::core::FieldAccessTypedExprPtr> sortingKeys,
      std::vector<velox::core::SortOrder> sortingOrders,
      bool pruneWhenEmpty,
      velox::RowTypePtr outputType,
      std::vector<std::vector<velox::column_index_t>> requiredColumns,
      std::vector<std::pair<velox::column_index_t, velox::column_index_t>>
          markerChannels,
      std::vector<PassThroughColumnSpecification> passThroughColumns,
      std::vector<velox::core::PlanNodePtr> sources);

  /// Returns the sources of this TableFunctionProcessorNode.
  /// This will be empty if the table function does not require input data,
  /// and will contain one source if the table function has an input table.
  /// If the table function has multiple input tables, then this will contain
  /// one source with a cross join of all input tables, and the function will
  /// use marker channels to identify the end of partition positions for each
  /// input table.
  const std::vector<velox::core::PlanNodePtr>& sources() const override {
    return sources_;
  }

  const velox::RowTypePtr& inputType() const {
    return sources_[0]->outputType();
  }

  bool canSpill(const velox::core::QueryConfig& queryConfig) const override {
    return false;
  }

  /// Specifies whether the function should be pruned or executed when the input
  /// is empty. pruneWhenEmpty is false if and only if all original input tables
  /// are KEEP WHEN EMPTY.
  bool pruneWhenEmpty() const {
    return pruneWhenEmpty_;
  }

  /// Specifies the output type of the TableFunctionProcessorNode.
  /// This is the same as the output type of the table function,
  /// but with additional columns for pass-through columns added.
  const velox::RowTypePtr& outputType() const override {
    return outputType_;
  };

  std::string_view name() const override {
    return "TableFunctionProcessor";
  }

  const std::string functionName() const {
    return functionName_;
  }

  const std::shared_ptr<const TableFunctionHandle> handle() const {
    return handle_;
  }

  const std::vector<velox::core::FieldAccessTypedExprPtr>& partitionKeys()
      const {
    return partitionKeys_;
  }

  const std::vector<velox::core::FieldAccessTypedExprPtr>& sortingKeys() const {
    return sortingKeys_;
  }

  const std::vector<velox::core::SortOrder>& sortingOrders() const {
    return sortingOrders_;
  }

  /// Columns required from each source, ordered as table argument
  /// specifications.
  const std::vector<std::vector<velox::column_index_t>>& requiredColumns()
      const {
    return requiredColumns_;
  }

  /// This mapping is present only for table functions with multiple input
  /// tables.
  /// It denotes a mapping from source columns to helper "marker" symbol which
  /// indicates whether the source value is valid for processing or for
  /// pass-through. null value in the marker column indicates that the value
  /// at the same position in the source column should not be processed or
  /// passed-through.
  ///
  /// Example:
  /// Given two input tables T1(a,b) PARTITION BY a and T2(c, d) PARTITION BY c
  /// T1 partitions:           T2 partitions:
  /// a  | b                   c  | d
  /// ---+---                  ---+---
  /// 1  | 10                  5  | 50
  /// 1  | 20                  5  | 60
  /// 1  | 30                  6  | 90
  /// 2  | 40                  6  | 100
  /// 2  | 50                  6  | 110
  ///
  /// TransformTableFunctionToTableFunctionProcessor planner rule creates a
  /// join that produces a cartesian product of partitions from each table,
  /// resulting in 4 partitions.
  /// Partition (a=1, c=5):
  /// a    | b    | marker_1 | c  | d   | marker_2
  /// -----+------+----------+----+-----+----------
  /// 1    | 10   | 1        | 5  | 50  | 1        (row 1 from both partitions)
  /// 1    | 20   | 2        | 5  | 60  | 2        (row 2 from both partitions)
  /// 1    | 30   | 3        | 5  | 50  | null     (filler row for T2, real row
  /// 3 from T1)
  ///
  /// Partition (a=1, c=6):
  /// a    | b    | marker_1 | c  | d   | marker_2
  /// -----+------+----------+----+-----+----------
  /// 1    | 10   | 1        | 6  | 90  | 1        (row 1 from both partitions)
  /// 1    | 20   | 2        | 6  | 100 | 2        (row 2 from both partitions)
  /// 1    | 30   | 3        | 6  | 110 | 3        (row 3 from both partitions)
  ///
  /// Partition (a=2, c=5):
  /// a    | b    | marker_1 | c  | d   | marker_2
  /// -----+------+----------+----+-----+----------
  /// 2    | 40   | 1        | 5  | 50  | 1        (row 1 from both partitions)
  /// 2    | 50   | 2        | 5  | 60  | 2        (row 2 from both partitions)
  ///
  /// Partition (a=2, c=6):
  /// a    | b    | marker_1 | c  | d   | marker_2
  /// -----+------+----------+----+-----+----------
  /// 2    | 40   | 1        | 6  | 90  | 1        (row 1 from both partitions)
  /// 2    | 50   | 2        | 6  | 100 | 2        (row 2 from both partitions)
  /// 2    | 40   | null     | 6  | 110 | 3        (filler row for T1, real row
  /// 3 from T2)
  ///
  /// markerVariables map:
  /// {
  /// VariableReferenceExpression(a) -> VariableReferenceExpression(marker_1),
  /// VariableReferenceExpression(b) -> VariableReferenceExpression(marker_1),
  /// VariableReferenceExpression(c) -> VariableReferenceExpression(marker_2),
  /// VariableReferenceExpression(d) -> VariableReferenceExpression(marker_2)
  /// }
  /// When marker_1 is null, columns a and b should not be processed or
  /// passed-through. When marker_2 is null, columns c and d should not be
  /// processed or passed-through.
  const std::vector<std::pair<velox::column_index_t, velox::column_index_t>>&
  markerChannels() const {
    return markerChannels_;
  }

  /// All source symbols to be produced on output, ordered as table argument
  /// specifications. For a table argument specification with pass-through
  /// columns, all columns of the corresponding source are included.
  /// For a table argument specification without pass-through columns,
  /// only the partitioning columns of the corresponding source are included.
  const std::vector<PassThroughColumnSpecification>& passThroughColumns()
      const {
    return passThroughColumns_;
  }

  bool requiresSplits() const override {
    return sources_.empty();
  }

  folly::dynamic serialize() const override;

  static velox::core::PlanNodePtr create(
      const folly::dynamic& obj,
      void* context);

 private:
  void addDetails(std::stringstream& stream) const override;

  const std::string functionName_;

  TableFunctionHandlePtr handle_;

  std::vector<velox::core::FieldAccessTypedExprPtr> partitionKeys_;
  std::vector<velox::core::FieldAccessTypedExprPtr> sortingKeys_;
  std::vector<velox::core::SortOrder> sortingOrders_;

  bool pruneWhenEmpty_;

  const velox::RowTypePtr outputType_;

  const std::vector<std::vector<velox::column_index_t>> requiredColumns_;

  const std::vector<std::pair<velox::column_index_t, velox::column_index_t>>
      markerChannels_;

  const std::vector<PassThroughColumnSpecification> passThroughColumns_;

  const std::vector<velox::core::PlanNodePtr> sources_;
};

using TableFunctionProcessorNodePtr =
    std::shared_ptr<const TableFunctionProcessorNode>;

} // namespace facebook::presto::tvf
