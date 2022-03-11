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

#include "velox/substrait/SubstraitToVeloxPlan.h"
#include "velox/substrait/TypeUtils.h"

namespace facebook::velox::substrait {

std::shared_ptr<const core::PlanNode> SubstraitVeloxPlanConverter::toVeloxPlan(
    const ::substrait::AggregateRel& sAgg) {
  std::shared_ptr<const core::PlanNode> childNode;
  if (sAgg.has_input()) {
    childNode = toVeloxPlan(sAgg.input());
  } else {
    VELOX_FAIL("Child Rel is expected in AggregateRel.");
  }

  // Construct Velox grouping expressions.
  auto inputTypes = childNode->outputType();
  std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>>
      veloxGroupingExprs;
  const auto& groupings = sAgg.groupings();
  int inputPlanNodeId = planNodeId_ - 1;
  // The index of output column.
  int outIdx = 0;
  for (const auto& grouping : groupings) {
    auto groupingExprs = grouping.grouping_expressions();
    for (const auto& groupingExpr : groupingExprs) {
      // Velox's groupings are limited to be Field, so groupingExpr is
      // expected to be FieldReference.
      auto fieldExpr = exprConverter_->toVeloxExpr(
          groupingExpr.selection(), inputPlanNodeId);
      veloxGroupingExprs.emplace_back(fieldExpr);
      outIdx += 1;
    }
  }

  // Parse measures to get Aggregation phase and expressions.
  bool phaseInited = false;
  core::AggregationNode::Step aggStep;
  // Project expressions are used to conduct a pre-projection before
  // Aggregation if needed.
  std::vector<std::shared_ptr<const core::ITypedExpr>> projectExprs;
  std::vector<std::string> projectOutNames;
  std::vector<std::shared_ptr<const core::CallTypedExpr>> aggExprs;
  aggExprs.reserve(sAgg.measures().size());

  // Construct Velox Aggregate expressions.
  for (const auto& sMea : sAgg.measures()) {
    auto aggFunction = sMea.measure();
    // Get the params of this Aggregate function.
    std::vector<std::shared_ptr<const core::ITypedExpr>> aggParams;
    auto args = aggFunction.args();
    aggParams.reserve(args.size());
    for (auto arg : args) {
      auto typeCase = arg.rex_type_case();
      switch (typeCase) {
        case ::substrait::Expression::RexTypeCase::kSelection: {
          aggParams.emplace_back(
              exprConverter_->toVeloxExpr(arg.selection(), inputPlanNodeId));
          break;
        }
        case ::substrait::Expression::RexTypeCase::kScalarFunction: {
          // Pre-projection is needed before Aggregate.
          // The input of Aggregatation will be the output of the
          // pre-projection.
          auto sFunc = arg.scalar_function();
          projectExprs.emplace_back(
              exprConverter_->toVeloxExpr(sFunc, inputPlanNodeId));
          auto colOutName = subParser_->makeNodeName(planNodeId_, outIdx);
          projectOutNames.emplace_back(colOutName);
          auto outType = subParser_->parseType(sFunc.output_type());
          auto aggInputParam =
              std::make_shared<const core::FieldAccessTypedExpr>(
                  toVeloxType(outType->type), colOutName);
          aggParams.emplace_back(aggInputParam);
          break;
        }
        default:
          VELOX_NYI(
              "Substrait conversion not supported for arg type '{}'", typeCase);
      }
    }
    auto funcId = aggFunction.function_reference();
    auto funcName = subParser_->findVeloxFunction(functionMap_, funcId);
    auto aggOutType = subParser_->parseType(aggFunction.output_type());
    auto aggExpr = std::make_shared<const core::CallTypedExpr>(
        toVeloxType(aggOutType->type), std::move(aggParams), funcName);
    aggExprs.emplace_back(aggExpr);

    // Initialize the Aggregate Step.
    if (!phaseInited) {
      auto phase = aggFunction.phase();
      switch (phase) {
        case ::substrait::AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE:
          aggStep = core::AggregationNode::Step::kPartial;
          break;
        case ::substrait::AGGREGATION_PHASE_INTERMEDIATE_TO_INTERMEDIATE:
          aggStep = core::AggregationNode::Step::kIntermediate;
          break;
        case ::substrait::AGGREGATION_PHASE_INTERMEDIATE_TO_RESULT:
          aggStep = core::AggregationNode::Step::kFinal;
          break;
        default:
          VELOX_NYI("Substrait conversion not supported for phase '{}'", phase);
      }
      phaseInited = true;
    }
    outIdx += 1;
  }

  // Construct the Aggregate Node.
  bool ignoreNullKeys = false;
  std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>> aggregateMasks(
      outIdx);
  std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>>
      preGroupingExprs;
  if (projectOutNames.size() == 0) {
    // Conduct Aggregation directly.
    std::vector<std::string> aggOutNames;
    aggOutNames.reserve(outIdx);
    for (int idx = 0; idx < outIdx; idx++) {
      aggOutNames.emplace_back(subParser_->makeNodeName(planNodeId_, idx));
    }
    return std::make_shared<core::AggregationNode>(
        nextPlanNodeId(),
        aggStep,
        veloxGroupingExprs,
        preGroupingExprs,
        aggOutNames,
        aggExprs,
        aggregateMasks,
        ignoreNullKeys,
        childNode);
  } else {
    // A Project Node is needed before Aggregation.
    auto projectNode = std::make_shared<core::ProjectNode>(
        nextPlanNodeId(),
        std::move(projectOutNames),
        std::move(projectExprs),
        childNode);
    std::vector<std::string> aggOutNames;
    aggOutNames.reserve(outIdx);
    for (int idx = 0; idx < outIdx; idx++) {
      aggOutNames.emplace_back(subParser_->makeNodeName(planNodeId_, idx));
    }
    return std::make_shared<core::AggregationNode>(
        nextPlanNodeId(),
        aggStep,
        veloxGroupingExprs,
        preGroupingExprs,
        aggOutNames,
        aggExprs,
        aggregateMasks,
        ignoreNullKeys,
        projectNode);
  }
}

std::shared_ptr<const core::PlanNode> SubstraitVeloxPlanConverter::toVeloxPlan(
    const ::substrait::ProjectRel& sProject) {
  std::shared_ptr<const core::PlanNode> childNode;
  if (sProject.has_input()) {
    childNode = toVeloxPlan(sProject.input());
  } else {
    VELOX_FAIL("Child Rel is expected in ProjectRel.");
  }

  // Construct Velox Expressions.
  auto projectExprs = sProject.expressions();
  std::vector<std::string> projectNames;
  std::vector<std::shared_ptr<const core::ITypedExpr>> expressions;
  projectNames.reserve(projectExprs.size());
  expressions.reserve(projectExprs.size());
  auto prePlanNodeId = planNodeId_ - 1;
  int colIdx = 0;
  for (const auto& expr : projectExprs) {
    expressions.emplace_back(exprConverter_->toVeloxExpr(expr, prePlanNodeId));
    projectNames.emplace_back(subParser_->makeNodeName(planNodeId_, colIdx));
    colIdx += 1;
  }

  auto projectNode = std::make_shared<core::ProjectNode>(
      nextPlanNodeId(),
      std::move(projectNames),
      std::move(expressions),
      childNode);
  return projectNode;
}

std::shared_ptr<const core::PlanNode> SubstraitVeloxPlanConverter::toVeloxPlan(
    const ::substrait::FilterRel& sFilter) {
  // TODO: Currently Filter is skipped because Filter is Pushdowned to
  // TableScan.
  std::shared_ptr<const core::PlanNode> childNode;
  if (sFilter.has_input()) {
    childNode = toVeloxPlan(sFilter.input());
  } else {
    VELOX_FAIL("Child Rel is expected in FilterRel.");
  }
  return childNode;
}

std::shared_ptr<const core::PlanNode> SubstraitVeloxPlanConverter::toVeloxPlan(
    const ::substrait::ReadRel& sRead,
    u_int32_t& index,
    std::vector<std::string>& paths,
    std::vector<u_int64_t>& starts,
    std::vector<u_int64_t>& lengths) {
  // Get output names and types.
  std::vector<std::string> colNameList;
  std::vector<TypePtr> veloxTypeList;
  if (sRead.has_base_schema()) {
    const auto& baseSchema = sRead.base_schema();
    colNameList.reserve(baseSchema.names().size());
    for (const auto& name : baseSchema.names()) {
      colNameList.emplace_back(name);
    }
    auto substraitTypeList = subParser_->parseNamedStruct(baseSchema);
    veloxTypeList.reserve(substraitTypeList.size());
    for (const auto& subType : substraitTypeList) {
      veloxTypeList.emplace_back(toVeloxType(subType->type));
    }
  }

  // Parse local files
  if (sRead.has_local_files()) {
    const auto& fileList = sRead.local_files().items();
    paths.reserve(fileList.size());
    starts.reserve(fileList.size());
    lengths.reserve(fileList.size());
    for (const auto& file : fileList) {
      // Expect all Partitions share the same index.
      index = file.partition_index();
      paths.emplace_back(file.uri_file());
      starts.emplace_back(file.start());
      lengths.emplace_back(file.length());
    }
  }

  // Velox requires Filter Pushdown must being enabled.
  bool filterPushdownEnabled = true;
  std::shared_ptr<connector::hive::HiveTableHandle> tableHandle;
  if (!sRead.has_filter()) {
    tableHandle = std::make_shared<connector::hive::HiveTableHandle>(
        filterPushdownEnabled, connector::hive::SubfieldFilters{}, nullptr);
  } else {
    connector::hive::SubfieldFilters filters =
        toVeloxFilter(colNameList, veloxTypeList, sRead.filter());
    tableHandle = std::make_shared<connector::hive::HiveTableHandle>(
        filterPushdownEnabled, std::move(filters), nullptr);
  }

  // Get assignments and out names.
  std::vector<std::string> outNames;
  outNames.reserve(colNameList.size());
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      assignments;
  for (int idx = 0; idx < colNameList.size(); idx++) {
    auto outName = subParser_->makeNodeName(planNodeId_, idx);
    assignments[outName] = std::make_shared<connector::hive::HiveColumnHandle>(
        colNameList[idx],
        connector::hive::HiveColumnHandle::ColumnType::kRegular,
        veloxTypeList[idx]);
    outNames.emplace_back(outName);
  }
  auto outputType = ROW(std::move(outNames), std::move(veloxTypeList));

  auto tableScanNode = std::make_shared<core::TableScanNode>(
      nextPlanNodeId(), outputType, tableHandle, assignments);
  return tableScanNode;
}

std::shared_ptr<const core::PlanNode> SubstraitVeloxPlanConverter::toVeloxPlan(
    const ::substrait::Rel& sRel) {
  if (sRel.has_aggregate()) {
    return toVeloxPlan(sRel.aggregate());
  }
  if (sRel.has_project()) {
    return toVeloxPlan(sRel.project());
  }
  if (sRel.has_filter()) {
    return toVeloxPlan(sRel.filter());
  }
  if (sRel.has_read()) {
    return toVeloxPlan(sRel.read(), partitionIndex_, paths_, starts_, lengths_);
  }
  VELOX_NYI("Substrait conversion not supported for Rel.");
}

std::shared_ptr<const core::PlanNode> SubstraitVeloxPlanConverter::toVeloxPlan(
    const ::substrait::RelRoot& sRoot) {
  // TODO: Use the names as the output names for the whole computing.
  const auto& sNames = sRoot.names();
  if (sRoot.has_input()) {
    const auto& sRel = sRoot.input();
    return toVeloxPlan(sRel);
  }
  VELOX_FAIL("Input is expected in RelRoot.");
}

std::shared_ptr<const core::PlanNode> SubstraitVeloxPlanConverter::toVeloxPlan(
    const ::substrait::Plan& sPlan) {
  // Construct the function map based on the Substrait representation.
  for (const auto& sExtension : sPlan.extensions()) {
    if (!sExtension.has_extension_function()) {
      continue;
    }
    const auto& sFmap = sExtension.extension_function();
    auto id = sFmap.function_anchor();
    auto name = sFmap.name();
    functionMap_[id] = name;
  }

  // Construct the expression converter.
  exprConverter_ =
      std::make_shared<SubstraitVeloxExprConverter>(subParser_, functionMap_);

  // In fact, only one RelRoot or Rel is expected here.
  for (const auto& sRel : sPlan.relations()) {
    if (sRel.has_root()) {
      return toVeloxPlan(sRel.root());
    }
    if (sRel.has_rel()) {
      return toVeloxPlan(sRel.rel());
    }
  }
  VELOX_FAIL("RelRoot or Rel is expected in Plan.");
}

std::string SubstraitVeloxPlanConverter::nextPlanNodeId() {
  auto id = fmt::format("{}", planNodeId_);
  planNodeId_++;
  return id;
}

// This class contains the needed infos for Filter Pushdown.
// TODO: Support different types here.
class FilterInfo {
 public:
  // Used to set the left bound.
  void setLeft(double left, bool isExclusive) {
    left_ = left;
    leftExclusive_ = isExclusive;
    if (!isInitialized_) {
      isInitialized_ = true;
    }
  }

  // Used to set the right bound.
  void setRight(double right, bool isExclusive) {
    right_ = right;
    rightExclusive_ = isExclusive;
    if (!isInitialized_) {
      isInitialized_ = true;
    }
  }

  // Will fordis Null value if called once.
  void forbidsNull() {
    nullAllowed_ = false;
    if (!isInitialized_) {
      isInitialized_ = true;
    }
  }

  // Return the initialization status.
  bool isInitialized() {
    return isInitialized_ ? true : false;
  }

  // The left bound.
  std::optional<double> left_ = std::nullopt;
  // The right bound.
  std::optional<double> right_ = std::nullopt;
  // The Null allowing.
  bool nullAllowed_ = true;
  // If true, left bound will be exclusive.
  bool leftExclusive_ = false;
  // If true, right bound will be exclusive.
  bool rightExclusive_ = false;

 private:
  bool isInitialized_ = false;
};

connector::hive::SubfieldFilters SubstraitVeloxPlanConverter::toVeloxFilter(
    const std::vector<std::string>& inputNameList,
    const std::vector<TypePtr>& inputTypeList,
    const ::substrait::Expression& sFilter) {
  connector::hive::SubfieldFilters filters;
  // A map between the column index and the FilterInfo for that column.
  std::unordered_map<int, std::shared_ptr<FilterInfo>> colInfoMap;
  for (int idx = 0; idx < inputNameList.size(); idx++) {
    colInfoMap[idx] = std::make_shared<FilterInfo>();
  }

  std::vector<::substrait::Expression_ScalarFunction> scalarFunctions;
  flattenConditions(sFilter, scalarFunctions);
  // Construct the FilterInfo for the related column.
  for (const auto& scalarFunction : scalarFunctions) {
    auto filterNameSpec = subParser_->findSubstraitFuncSpec(
        functionMap_, scalarFunction.function_reference());
    auto filterName = subParser_->getSubFunctionName(filterNameSpec);
    int32_t colIdx;
    // TODO: Add different types' support here.
    double val;
    for (auto& param : scalarFunction.args()) {
      auto typeCase = param.rex_type_case();
      switch (typeCase) {
        case ::substrait::Expression::RexTypeCase::kSelection: {
          auto sel = param.selection();
          // TODO: Only direct reference is considered here.
          auto dRef = sel.direct_reference();
          colIdx = subParser_->parseReferenceSegment(dRef);
          break;
        }
        case ::substrait::Expression::RexTypeCase::kLiteral: {
          auto sLit = param.literal();
          // TODO: Only double is considered here.
          val = sLit.fp64();
          break;
        }
        default:
          VELOX_NYI(
              "Substrait conversion not supported for arg type '{}'", typeCase);
      }
    }
    if (filterName == "is_not_null") {
      colInfoMap[colIdx]->forbidsNull();
    } else if (filterName == "gte") {
      colInfoMap[colIdx]->setLeft(val, false);
    } else if (filterName == "gt") {
      colInfoMap[colIdx]->setLeft(val, true);
    } else if (filterName == "lte") {
      colInfoMap[colIdx]->setRight(val, false);
    } else if (filterName == "lt") {
      colInfoMap[colIdx]->setRight(val, true);
    } else {
      VELOX_NYI(
          "Substrait conversion not supported for filter name '{}'",
          filterName);
    }
  }

  // Construct the Filters.
  for (int idx = 0; idx < inputNameList.size(); idx++) {
    auto filterInfo = colInfoMap[idx];
    double leftBound;
    double rightBound;
    bool leftUnbounded = true;
    bool rightUnbounded = true;
    bool leftExclusive = false;
    bool rightExclusive = false;
    if (filterInfo->isInitialized()) {
      if (filterInfo->left_) {
        leftUnbounded = false;
        leftBound = filterInfo->left_.value();
        leftExclusive = filterInfo->leftExclusive_;
      }
      if (filterInfo->right_) {
        rightUnbounded = false;
        rightBound = filterInfo->right_.value();
        rightExclusive = filterInfo->rightExclusive_;
      }
      bool nullAllowed = filterInfo->nullAllowed_;
      filters[common::Subfield(inputNameList[idx])] =
          std::make_unique<common::DoubleRange>(
              leftBound,
              leftUnbounded,
              leftExclusive,
              rightBound,
              rightUnbounded,
              rightExclusive,
              nullAllowed);
    }
  }
  return filters;
}

void SubstraitVeloxPlanConverter::flattenConditions(
    const ::substrait::Expression& sFilter,
    std::vector<::substrait::Expression_ScalarFunction>& scalarFunctions) {
  auto typeCase = sFilter.rex_type_case();
  switch (typeCase) {
    case ::substrait::Expression::RexTypeCase::kScalarFunction: {
      auto sFunc = sFilter.scalar_function();
      auto filterNameSpec = subParser_->findSubstraitFuncSpec(
          functionMap_, sFunc.function_reference());
      // TODO: Only and relation is supported here.
      if (subParser_->getSubFunctionName(filterNameSpec) == "and") {
        for (const auto& sCondition : sFunc.args()) {
          flattenConditions(sCondition, scalarFunctions);
        }
      } else {
        scalarFunctions.emplace_back(sFunc);
      }
      break;
    }
    default:
      VELOX_NYI("GetFlatConditions not supported for type '{}'", typeCase);
  }
}

} // namespace facebook::velox::substrait
