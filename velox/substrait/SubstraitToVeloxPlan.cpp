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
#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::substrait {

namespace {
template <TypeKind KIND>
VectorPtr setVectorFromVariantsByKind(
    const std::vector<velox::variant>& value,
    memory::MemoryPool* pool) {
  using T = typename TypeTraits<KIND>::NativeType;

  auto flatVector = std::dynamic_pointer_cast<FlatVector<T>>(
      BaseVector::create(CppToType<T>::create(), value.size(), pool));

  for (vector_size_t i = 0; i < value.size(); i++) {
    if (value[i].isNull()) {
      flatVector->setNull(i, true);
    } else {
      flatVector->set(i, value[i].value<T>());
    }
  }
  return flatVector;
}

template <>
VectorPtr setVectorFromVariantsByKind<TypeKind::VARBINARY>(
    const std::vector<velox::variant>& value,
    memory::MemoryPool* pool) {
  throw std::invalid_argument("Return of VARBINARY data is not supported");
}

template <>
VectorPtr setVectorFromVariantsByKind<TypeKind::VARCHAR>(
    const std::vector<velox::variant>& value,
    memory::MemoryPool* pool) {
  auto flatVector = std::dynamic_pointer_cast<FlatVector<StringView>>(
      BaseVector::create(VARCHAR(), value.size(), pool));

  for (vector_size_t i = 0; i < value.size(); i++) {
    if (value[i].isNull()) {
      flatVector->setNull(i, true);
    } else {
      flatVector->set(i, StringView(value[i].value<Varchar>()));
    }
  }
  return flatVector;
}

VectorPtr setVectorFromVariants(
    const TypePtr& type,
    const std::vector<velox::variant>& value,
    velox::memory::MemoryPool* pool) {
  return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
      setVectorFromVariantsByKind, type->kind(), value, pool);
}
} // namespace

core::PlanNodePtr SubstraitVeloxPlanConverter::toVeloxPlan(
    const ::substrait::AggregateRel& aggRel,
    memory::MemoryPool* pool) {
  core::PlanNodePtr childNode;
  if (aggRel.has_input()) {
    childNode = toVeloxPlan(aggRel.input(), pool);
  } else {
    VELOX_FAIL("Child Rel is expected in AggregateRel.");
  }

  // Construct Velox grouping expressions.
  auto inputTypes = childNode->outputType();
  std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>>
      veloxGroupingExprs;

  const auto& groupings = aggRel.groupings();
  int inputPlanNodeId = planNodeId_ - 1;
  // The index of output column.
  int outIdx = 0;
  for (const auto& grouping : groupings) {
    auto groupingExprs = grouping.grouping_expressions();
    for (const auto& groupingExpr : groupingExprs) {
      // Velox's groupings are limited to be Field, so groupingExpr is
      // expected to be FieldReference.
      auto fieldExpr =
          exprConverter_->toVeloxExpr(groupingExpr.selection(), inputTypes);
      veloxGroupingExprs.emplace_back(fieldExpr);
      outIdx += 1;
    }
  }

  // Parse measures to get Aggregation phase and expressions.
  bool phaseInited = false;
  core::AggregationNode::Step aggStep;
  // Project expressions are used to conduct a pre-projection before
  // Aggregation if needed.
  std::vector<core::TypedExprPtr> projectExprs;
  std::vector<std::string> projectOutNames;
  std::vector<std::shared_ptr<const core::CallTypedExpr>> aggExprs;
  aggExprs.reserve(aggRel.measures().size());

  // Construct Velox Aggregate expressions.
  for (const auto& sMea : aggRel.measures()) {
    auto aggFunction = sMea.measure();
    // Get the params of this Aggregate function.
    std::vector<core::TypedExprPtr> aggParams;
    auto args = aggFunction.args();
    aggParams.reserve(args.size());
    for (auto arg : args) {
      auto typeCase = arg.rex_type_case();
      switch (typeCase) {
        case ::substrait::Expression::RexTypeCase::kSelection: {
          aggParams.emplace_back(
              exprConverter_->toVeloxExpr(arg.selection(), inputTypes));
          break;
        }
        case ::substrait::Expression::RexTypeCase::kScalarFunction: {
          // Pre-projection is needed before Aggregate.
          // The input of Aggregatation will be the output of the
          // pre-projection.
          auto sFunc = arg.scalar_function();
          projectExprs.emplace_back(
              exprConverter_->toVeloxExpr(sFunc, inputTypes));
          auto colOutName = substraitParser_->makeNodeName(planNodeId_, outIdx);
          projectOutNames.emplace_back(colOutName);
          auto outType = substraitParser_->parseType(sFunc.output_type());
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
    auto funcName = substraitParser_->findVeloxFunction(functionMap_, funcId);
    auto aggOutType = substraitParser_->parseType(aggFunction.output_type());
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
      aggOutNames.emplace_back(
          substraitParser_->makeNodeName(planNodeId_, idx));
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
      aggOutNames.emplace_back(
          substraitParser_->makeNodeName(planNodeId_, idx));
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

core::PlanNodePtr SubstraitVeloxPlanConverter::toVeloxPlan(
    const ::substrait::ProjectRel& projectRel,
    memory::MemoryPool* pool) {
  core::PlanNodePtr childNode;
  if (projectRel.has_input()) {
    childNode = toVeloxPlan(projectRel.input(), pool);
  } else {
    VELOX_FAIL("Child Rel is expected in ProjectRel.");
  }

  // Construct Velox Expressions.
  auto projectExprs = projectRel.expressions();
  std::vector<std::string> projectNames;
  std::vector<core::TypedExprPtr> expressions;
  projectNames.reserve(projectExprs.size());
  expressions.reserve(projectExprs.size());

  const auto& inputType = childNode->outputType();
  int colIdx = 0;
  for (const auto& expr : projectExprs) {
    expressions.emplace_back(exprConverter_->toVeloxExpr(expr, inputType));
    projectNames.emplace_back(
        substraitParser_->makeNodeName(planNodeId_, colIdx));
    colIdx += 1;
  }

  auto projectNode = std::make_shared<core::ProjectNode>(
      nextPlanNodeId(),
      std::move(projectNames),
      std::move(expressions),
      childNode);
  return projectNode;
}

core::PlanNodePtr SubstraitVeloxPlanConverter::toVeloxPlan(
    const ::substrait::FilterRel& filterRel,
    memory::MemoryPool* pool) {
  core::PlanNodePtr childNode;
  if (filterRel.has_input()) {
    childNode = toVeloxPlan(filterRel.input(), pool);
  } else {
    VELOX_FAIL("Child Rel is expected in FilterRel.");
  }

  const auto& inputType = childNode->outputType();
  const auto& sExpr = filterRel.condition();

  return std::make_shared<core::FilterNode>(
      nextPlanNodeId(),
      exprConverter_->toVeloxExpr(sExpr, inputType),
      childNode);
}

core::PlanNodePtr SubstraitVeloxPlanConverter::toVeloxPlan(
    const ::substrait::ReadRel& readRel,
    memory::MemoryPool* pool,
    u_int32_t& index,
    std::vector<std::string>& paths,
    std::vector<u_int64_t>& starts,
    std::vector<u_int64_t>& lengths) {
  // Get output names and types.
  std::vector<std::string> colNameList;
  std::vector<TypePtr> veloxTypeList;
  if (readRel.has_base_schema()) {
    const auto& baseSchema = readRel.base_schema();
    colNameList.reserve(baseSchema.names().size());
    for (const auto& name : baseSchema.names()) {
      colNameList.emplace_back(name);
    }
    auto substraitTypeList = substraitParser_->parseNamedStruct(baseSchema);
    veloxTypeList.reserve(substraitTypeList.size());
    for (const auto& substraitType : substraitTypeList) {
      veloxTypeList.emplace_back(toVeloxType(substraitType->type));
    }
  }

  // Parse local files
  if (readRel.has_local_files()) {
    const auto& fileList = readRel.local_files().items();
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

  // Do not hard-code connector ID and allow for connectors other than Hive.
  static const std::string kHiveConnectorId = "test-hive";

  // Velox requires Filter Pushdown must being enabled.
  bool filterPushdownEnabled = true;
  std::shared_ptr<connector::hive::HiveTableHandle> tableHandle;
  if (!readRel.has_filter()) {
    tableHandle = std::make_shared<connector::hive::HiveTableHandle>(
        kHiveConnectorId,
        "hive_table",
        filterPushdownEnabled,
        connector::hive::SubfieldFilters{},
        nullptr);
  } else {
    connector::hive::SubfieldFilters filters =
        toVeloxFilter(colNameList, veloxTypeList, readRel.filter());
    tableHandle = std::make_shared<connector::hive::HiveTableHandle>(
        kHiveConnectorId,
        "hive_table",
        filterPushdownEnabled,
        std::move(filters),
        nullptr);
  }

  // Get assignments and out names.
  std::vector<std::string> outNames;
  outNames.reserve(colNameList.size());
  std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
      assignments;
  for (int idx = 0; idx < colNameList.size(); idx++) {
    auto outName = substraitParser_->makeNodeName(planNodeId_, idx);
    assignments[outName] = std::make_shared<connector::hive::HiveColumnHandle>(
        colNameList[idx],
        connector::hive::HiveColumnHandle::ColumnType::kRegular,
        veloxTypeList[idx]);
    outNames.emplace_back(outName);
  }
  auto outputType = ROW(std::move(outNames), std::move(veloxTypeList));

  if (readRel.has_virtual_table()) {
    return toVeloxPlan(readRel, pool, outputType);

  } else {
    auto tableScanNode = std::make_shared<core::TableScanNode>(
        nextPlanNodeId(), outputType, tableHandle, assignments);
    return tableScanNode;
  }
}

core::PlanNodePtr SubstraitVeloxPlanConverter::toVeloxPlan(
    const ::substrait::ReadRel& readRel,
    memory::MemoryPool* pool,
    const RowTypePtr& type) {
  ::substrait::ReadRel_VirtualTable readVirtualTable = readRel.virtual_table();
  int64_t numVectors = readVirtualTable.values_size();
  int64_t numColumns = type->size();
  int64_t valueFieldNums =
      readVirtualTable.values(numVectors - 1).fields_size();
  std::vector<RowVectorPtr> vectors;
  vectors.reserve(numVectors);

  int64_t batchSize = valueFieldNums / numColumns;

  for (int64_t index = 0; index < numVectors; ++index) {
    std::vector<VectorPtr> children;
    ::substrait::Expression_Literal_Struct rowValue =
        readRel.virtual_table().values(index);
    auto fieldSize = rowValue.fields_size();
    VELOX_CHECK_EQ(fieldSize, batchSize * numColumns);

    for (int64_t col = 0; col < numColumns; ++col) {
      const TypePtr& outputChildType = type->childAt(col);
      std::vector<variant> batchChild;
      batchChild.reserve(batchSize);
      for (int64_t batchId = 0; batchId < batchSize; batchId++) {
        // each value in the batch
        auto fieldIdx = col * batchSize + batchId;
        ::substrait::Expression_Literal field = rowValue.fields(fieldIdx);

        auto expr = exprConverter_->toVeloxExpr(field);
        if (auto constantExpr =
                std::dynamic_pointer_cast<const core::ConstantTypedExpr>(
                    expr)) {
          if (!constantExpr->hasValueVector()) {
            batchChild.emplace_back(constantExpr->value());
          } else {
            VELOX_UNSUPPORTED(
                "Values node with complex type values is not supported yet");
          }
        } else {
          VELOX_FAIL("Expected constant expression");
        }
      }
      children.emplace_back(
          setVectorFromVariants(outputChildType, batchChild, pool));
    }

    vectors.emplace_back(
        std::make_shared<RowVector>(pool, type, nullptr, batchSize, children));
  }

  return std::make_shared<core::ValuesNode>(nextPlanNodeId(), vectors);
}

core::PlanNodePtr SubstraitVeloxPlanConverter::toVeloxPlan(
    const ::substrait::Rel& rel,
    memory::MemoryPool* pool) {
  if (rel.has_aggregate()) {
    return toVeloxPlan(rel.aggregate(), pool);
  }
  if (rel.has_project()) {
    return toVeloxPlan(rel.project(), pool);
  }
  if (rel.has_filter()) {
    return toVeloxPlan(rel.filter(), pool);
  }
  if (rel.has_read()) {
    return toVeloxPlan(
        rel.read(), pool, partitionIndex_, paths_, starts_, lengths_);
  }
  VELOX_NYI("Substrait conversion not supported for Rel.");
}

core::PlanNodePtr SubstraitVeloxPlanConverter::toVeloxPlan(
    const ::substrait::RelRoot& root,
    memory::MemoryPool* pool) {
  // TODO: Use the names as the output names for the whole computing.
  const auto& names = root.names();
  if (root.has_input()) {
    const auto& rel = root.input();
    return toVeloxPlan(rel, pool);
  }
  VELOX_FAIL("Input is expected in RelRoot.");
}

core::PlanNodePtr SubstraitVeloxPlanConverter::toVeloxPlan(
    const ::substrait::Plan& substraitPlan,
    memory::MemoryPool* pool) {
  // Construct the function map based on the Substrait representation.
  constructFunctionMap(substraitPlan);

  // Construct the expression converter.
  exprConverter_ = std::make_shared<SubstraitVeloxExprConverter>(functionMap_);

  // In fact, only one RelRoot or Rel is expected here.
  for (const auto& rel : substraitPlan.relations()) {
    if (rel.has_root()) {
      return toVeloxPlan(rel.root(), pool);
    }
    if (rel.has_rel()) {
      return toVeloxPlan(rel.rel(), pool);
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
    const ::substrait::Expression& substraitFilter) {
  connector::hive::SubfieldFilters filters;
  // A map between the column index and the FilterInfo for that column.
  std::unordered_map<int, std::shared_ptr<FilterInfo>> colInfoMap;
  for (int idx = 0; idx < inputNameList.size(); idx++) {
    colInfoMap[idx] = std::make_shared<FilterInfo>();
  }

  std::vector<::substrait::Expression_ScalarFunction> scalarFunctions;
  flattenConditions(substraitFilter, scalarFunctions);
  // Construct the FilterInfo for the related column.
  for (const auto& scalarFunction : scalarFunctions) {
    auto filterNameSpec = substraitParser_->findFunctionSpec(
        functionMap_, scalarFunction.function_reference());
    auto filterName = substraitParser_->getFunctionName(filterNameSpec);
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
          colIdx = substraitParser_->parseReferenceSegment(dRef);
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
    const ::substrait::Expression& substraitFilter,
    std::vector<::substrait::Expression_ScalarFunction>& scalarFunctions) {
  auto typeCase = substraitFilter.rex_type_case();
  switch (typeCase) {
    case ::substrait::Expression::RexTypeCase::kScalarFunction: {
      auto sFunc = substraitFilter.scalar_function();
      auto filterNameSpec = substraitParser_->findFunctionSpec(
          functionMap_, sFunc.function_reference());
      // TODO: Only and relation is supported here.
      if (substraitParser_->getFunctionName(filterNameSpec) == "and") {
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

void SubstraitVeloxPlanConverter::constructFunctionMap(
    const ::substrait::Plan& substraitPlan) {
  // Construct the function map based on the Substrait representation.
  for (const auto& sExtension : substraitPlan.extensions()) {
    if (!sExtension.has_extension_function()) {
      continue;
    }
    const auto& sFmap = sExtension.extension_function();
    auto id = sFmap.function_anchor();
    auto name = sFmap.name();
    functionMap_[id] = name;
  }
}

const std::string& SubstraitVeloxPlanConverter::findFunction(
    uint64_t id) const {
  return substraitParser_->findFunctionSpec(functionMap_, id);
}

} // namespace facebook::velox::substrait
