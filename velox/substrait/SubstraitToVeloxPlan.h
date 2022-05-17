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

#pragma once

#include "velox/connectors/hive/HiveConnector.h"
#include "velox/core/PlanNode.h"
#include "velox/substrait/SubstraitToVeloxExpr.h"

namespace facebook::velox::substrait {

/// This class is used to convert the Substrait plan into Velox plan.
class SubstraitVeloxPlanConverter {
 public:
  /// Convert Substrait AggregateRel into Velox PlanNode.
  core::PlanNodePtr toVeloxPlan(
      const ::substrait::AggregateRel& aggRel,
      memory::MemoryPool* pool);

  /// Convert Substrait ProjectRel into Velox PlanNode.
  core::PlanNodePtr toVeloxPlan(
      const ::substrait::ProjectRel& projectRel,
      memory::MemoryPool* pool);

  /// Convert Substrait FilterRel into Velox PlanNode.
  core::PlanNodePtr toVeloxPlan(
      const ::substrait::FilterRel& filterRel,
      memory::MemoryPool* pool);

  /// Convert Substrait ReadRel into Velox PlanNode.
  /// Index: the index of the partition this item belongs to.
  /// Starts: the start positions in byte to read from the items.
  /// Lengths: the lengths in byte to read from the items.
  core::PlanNodePtr toVeloxPlan(
      const ::substrait::ReadRel& readRel,
      memory::MemoryPool* pool,
      u_int32_t& index,
      std::vector<std::string>& paths,
      std::vector<u_int64_t>& starts,
      std::vector<u_int64_t>& lengths);

  /// Convert Substrait ReadRel into Velox Values Node.
  core::PlanNodePtr toVeloxPlan(
      const ::substrait::ReadRel& readRel,
      memory::MemoryPool* pool,
      const RowTypePtr& type);

  /// Convert Substrait Rel into Velox PlanNode.
  core::PlanNodePtr toVeloxPlan(
      const ::substrait::Rel& rel,
      memory::MemoryPool* pool);

  /// Convert Substrait RelRoot into Velox PlanNode.
  core::PlanNodePtr toVeloxPlan(
      const ::substrait::RelRoot& root,
      memory::MemoryPool* pool);

  /// Convert Substrait Plan into Velox PlanNode.
  core::PlanNodePtr toVeloxPlan(
      const ::substrait::Plan& substraitPlan,
      memory::MemoryPool* pool);

  /// Construct the function map between the index and the Substrait function
  /// name.
  void constructFunctionMap(const ::substrait::Plan& sPlan);

  /// Return the function map used by this plan converter.
  const std::unordered_map<uint64_t, std::string>& getFunctionMap() const {
    return functionMap_;
  }

  /// Return the index of Partition to be scanned.
  u_int32_t getPartitionIndex() {
    return partitionIndex_;
  }

  /// Return the paths of the files to be scanned.
  const std::vector<std::string>& getPaths() {
    return paths_;
  }

  /// Return the starts of the files to be scanned.
  const std::vector<u_int64_t>& getStarts() {
    return starts_;
  }

  /// Return the lengths to be scanned for each file.
  const std::vector<u_int64_t>& getLengths() {
    return lengths_;
  }

  /// Looks up a function by ID and returns function name if found. Throws if
  /// function with specified ID doesn't exist. Returns a compound
  /// function specification consisting of the function name and the input
  /// types. The format is as follows: <function
  /// name>:<arg_type0>_<arg_type1>_..._<arg_typeN>
  const std::string& findFunction(uint64_t id) const;

 private:
  /// Returns unique ID to use for plan node. Produces sequential numbers
  /// starting from zero.
  std::string nextPlanNodeId();

  /// Used to convert Substrait Filter into Velox SubfieldFilters which will
  /// be used in TableScan.
  connector::hive::SubfieldFilters toVeloxFilter(
      const std::vector<std::string>& inputNameList,
      const std::vector<TypePtr>& inputTypeList,
      const ::substrait::Expression& substraitFilter);

  /// Multiple conditions are connected to a binary tree structure with
  /// the relation key words, including AND, OR, and etc. Currently, only
  /// AND is supported. This function is used to extract all the Substrait
  /// conditions in the binary tree structure into a vector.
  void flattenConditions(
      const ::substrait::Expression& substraitFilter,
      std::vector<::substrait::Expression_ScalarFunction>& scalarFunctions);

  /// The Substrait parser used to convert Substrait representations into
  /// recognizable representations.
  std::shared_ptr<SubstraitParser> substraitParser_{
      std::make_shared<SubstraitParser>()};

  /// The Expression converter used to convert Substrait representations into
  /// Velox expressions.
  std::shared_ptr<SubstraitVeloxExprConverter> exprConverter_;

  /// The Partition index.
  u_int32_t partitionIndex_;

  /// The file paths to be scanned.
  std::vector<std::string> paths_;

  /// The file starts in the scan.
  std::vector<u_int64_t> starts_;

  /// The lengths to be scanned.
  std::vector<u_int64_t> lengths_;

  /// The unique identification for each PlanNode.
  int planNodeId_ = 0;

  /// The map storing the relations between the function id and the function
  /// name. Will be constructed based on the Substrait representation.
  std::unordered_map<uint64_t, std::string> functionMap_;
};

} // namespace facebook::velox::substrait
