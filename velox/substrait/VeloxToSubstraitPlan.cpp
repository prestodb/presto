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

#include "velox/substrait/VeloxToSubstraitPlan.h"

namespace facebook::velox::substrait {

namespace {
::substrait::AggregationPhase toAggregationPhase(
    core::AggregationNode::Step step) {
  switch (step) {
    case core::AggregationNode::Step::kPartial: {
      return ::substrait::AGGREGATION_PHASE_INITIAL_TO_INTERMEDIATE;
    }
    case core::AggregationNode::Step::kIntermediate: {
      return ::substrait::AGGREGATION_PHASE_INTERMEDIATE_TO_INTERMEDIATE;
    }
    case core::AggregationNode::Step::kSingle: {
      return ::substrait::AGGREGATION_PHASE_INITIAL_TO_RESULT;
    }
    case core::AggregationNode::Step::kFinal: {
      return ::substrait::AGGREGATION_PHASE_INTERMEDIATE_TO_RESULT;
    }
    default:
      VELOX_NYI(
          "Unsupported Aggregate Step '{}' in Substrait ",
          mapAggregationStepToName(step));
  }
}

} // namespace

::substrait::Plan& VeloxToSubstraitPlanConvertor::toSubstrait(
    google::protobuf::Arena& arena,
    const core::PlanNodePtr& plan) {
  // Assume only accepts a single plan fragment.

  // Construct the function map based on the Velox plan.
  constructFunctionMap();

  // Construct the expression converter.
  exprConvertor_ =
      std::make_shared<VeloxToSubstraitExprConvertor>(functionMap_);

  ::substrait::Plan* substraitPlan =
      google::protobuf::Arena::CreateMessage<::substrait::Plan>(&arena);

  // Add Extension Functions.
  substraitPlan->MergeFrom(addExtensionFunc(arena));

  // Add unknown type in extension.
  auto unknownType = substraitPlan->add_extensions()->mutable_extension_type();

  unknownType->set_extension_uri_reference(0);
  unknownType->set_type_anchor(0);
  unknownType->set_name("UNKNOWN");

  // Do conversion.
  ::substrait::RelRoot* rootRel =
      substraitPlan->add_relations()->mutable_root();

  toSubstrait(arena, plan, rootRel->mutable_input());
  // Set RootRel names.
  for (const auto& name : plan->outputType()->names()) {
    rootRel->add_names(name);
  }

  return *substraitPlan;
}

void VeloxToSubstraitPlanConvertor::toSubstrait(
    google::protobuf::Arena& arena,
    const core::PlanNodePtr& planNode,
    ::substrait::Rel* rel) {
  if (auto filterNode =
          std::dynamic_pointer_cast<const core::FilterNode>(planNode)) {
    auto filterRel = rel->mutable_filter();
    toSubstrait(arena, filterNode, filterRel);
    return;
  }
  if (auto valuesNode =
          std::dynamic_pointer_cast<const core::ValuesNode>(planNode)) {
    ::substrait::ReadRel* readRel = rel->mutable_read();
    toSubstrait(arena, valuesNode, readRel);
    return;
  }
  if (auto projectNode =
          std::dynamic_pointer_cast<const core::ProjectNode>(planNode)) {
    ::substrait::ProjectRel* projectRel = rel->mutable_project();
    toSubstrait(arena, projectNode, projectRel);
    return;
  }
  if (auto aggregationNode =
          std::dynamic_pointer_cast<const core::AggregationNode>(planNode)) {
    ::substrait::AggregateRel* aggregateRel = rel->mutable_aggregate();
    toSubstrait(arena, aggregationNode, aggregateRel);
    return;
  }
}

void VeloxToSubstraitPlanConvertor::toSubstrait(
    google::protobuf::Arena& arena,
    const std::shared_ptr<const core::FilterNode>& filterNode,
    ::substrait::FilterRel* filterRel) {
  std::vector<core::PlanNodePtr> sources = filterNode->sources();

  // Check there only have one input.
  VELOX_USER_CHECK_EQ(
      1, sources.size(), "Filter plan node must have exactly one source.");
  const auto& source = sources[0];

  ::substrait::Rel* filterInput = filterRel->mutable_input();
  // Build source.
  toSubstrait(arena, source, filterInput);

  // Construct substrait expr(Filter condition).
  auto filterCondition = filterNode->filter();
  auto inputType = source->outputType();
  filterRel->mutable_condition()->MergeFrom(
      exprConvertor_->toSubstraitExpr(arena, filterCondition, inputType));

  filterRel->mutable_common()->mutable_direct();
}

void VeloxToSubstraitPlanConvertor::toSubstrait(
    google::protobuf::Arena& arena,
    const std::shared_ptr<const core::ValuesNode>& valuesNode,
    ::substrait::ReadRel* readRel) {
  const auto& outputType = valuesNode->outputType();

  ::substrait::ReadRel_VirtualTable* virtualTable =
      readRel->mutable_virtual_table();

  // The row number of the input data.
  int64_t numVectors = valuesNode->values().size();

  // There can be multiple rows in the data and each row is a RowVectorPtr.
  for (int64_t row = 0; row < numVectors; ++row) {
    // The row data.
    ::substrait::Expression_Literal_Struct* litValue =
        virtualTable->add_values();
    const auto& rowVector = valuesNode->values().at(row);
    // The column number of the row data.
    int64_t numColumns = rowVector->childrenSize();

    for (int64_t column = 0; column < numColumns; ++column) {
      ::substrait::Expression_Literal* substraitField =
          google::protobuf::Arena::CreateMessage<
              ::substrait::Expression_Literal>(&arena);

      const VectorPtr& child = rowVector->childAt(column);

      substraitField->MergeFrom(exprConvertor_->toSubstraitExpr(
          arena, std::make_shared<core::ConstantTypedExpr>(child), litValue));
    }
  }

  readRel->mutable_base_schema()->MergeFrom(
      typeConvertor_->toSubstraitNamedStruct(arena, outputType));

  readRel->mutable_common()->mutable_direct();
}

void VeloxToSubstraitPlanConvertor::toSubstrait(
    google::protobuf::Arena& arena,
    const std::shared_ptr<const core::ProjectNode>& projectNode,
    ::substrait::ProjectRel* projectRel) {
  std::vector<std::shared_ptr<const core::ITypedExpr>> projections =
      projectNode->projections();

  std::vector<core::PlanNodePtr> sources = projectNode->sources();
  // Check there only have one input.
  VELOX_USER_CHECK_EQ(
      1, sources.size(), "Project plan node must have exactly one source.");
  // The previous node.
  const auto& source = sources[0];

  // Process the source Node.
  ::substrait::Rel* projectRelInput = projectRel->mutable_input();
  toSubstrait(arena, source, projectRelInput);

  // Remap the output.
  ::substrait::RelCommon_Emit* projRelEmit =
      projectRel->mutable_common()->mutable_emit();

  int64_t projectionSize = projections.size();

  auto inputType = source->outputType();
  int64_t inputTypeSize = inputType->size();

  for (int64_t i = 0; i < projectionSize; i++) {
    const auto& veloxExpr = projections.at(i);

    projectRel->add_expressions()->MergeFrom(
        exprConvertor_->toSubstraitExpr(arena, veloxExpr, inputType));

    // Add outputMapping for each expression.
    projRelEmit->add_output_mapping(inputTypeSize + i);
  }

  return;
}

void VeloxToSubstraitPlanConvertor::toSubstrait(
    google::protobuf::Arena& arena,
    const std::shared_ptr<const core::AggregationNode>& aggregateNode,
    ::substrait::AggregateRel* aggregateRel) {
  // Process the source Node.
  const auto& sources = aggregateNode->sources();
  // Check there only have one input.
  VELOX_USER_CHECK_EQ(
      1, sources.size(), "Aggregation plan node must have exactly one source.");
  const auto& source = sources[0];

  // Build source.
  toSubstrait(arena, source, aggregateRel->mutable_input());

  // Convert aggregate grouping keys, such as: group by key1, key2.
  auto inputType = source->outputType();
  auto groupingKeys = aggregateNode->groupingKeys();
  int64_t groupingKeySize = groupingKeys.size();
  ::substrait::AggregateRel_Grouping* aggGroupings =
      aggregateRel->add_groupings();

  for (int64_t i = 0; i < groupingKeySize; i++) {
    aggGroupings->add_grouping_expressions()->MergeFrom(
        exprConvertor_->toSubstraitExpr(
            arena,
            std::dynamic_pointer_cast<const core::ITypedExpr>(
                groupingKeys.at(i)),
            inputType));
  }

  // AggregatesSize should be equal to or greater than the aggregateMasks Size.
  // Two cases: 1. aggregateMasksSize = 0, aggregatesSize > aggregateMasksSize.
  // 2. aggregateMasksSize != 0, aggregatesSize = aggregateMasksSize.
  auto aggregates = aggregateNode->aggregates();
  auto aggregateMasks = aggregateNode->aggregateMasks();
  int64_t aggregatesSize = aggregates.size();
  int64_t aggregateMasksSize = aggregateMasks.size();
  VELOX_CHECK_GE(aggregatesSize, aggregateMasksSize);

  for (int64_t i = 0; i < aggregatesSize; i++) {
    ::substrait::AggregateRel_Measure* aggMeasures =
        aggregateRel->add_measures();

    auto aggMaskExpr = aggregateMasks.at(i);
    // Set substrait filter.
    ::substrait::Expression* aggFilter = aggMeasures->mutable_filter();
    if (aggMaskExpr.get()) {
      aggFilter->MergeFrom(exprConvertor_->toSubstraitExpr(
          arena,
          std::dynamic_pointer_cast<const core::ITypedExpr>(aggMaskExpr),
          inputType));
    } else {
      // Set null.
      aggFilter = nullptr;
    }

    // Process measure, eg:sum(a).
    const auto& aggregatesExpr = aggregates.at(i);
    ::substrait::AggregateFunction* aggFunction =
        aggMeasures->mutable_measure();

    // Aggregation function name.
    const auto& funName = aggregatesExpr->name();
    // set aggFunction args.
    for (const auto& expr : aggregatesExpr->inputs()) {
      // If the expr is CallTypedExpr, people need to do project firstly.
      if (auto aggregatesExprInput =
              std::dynamic_pointer_cast<const core::CallTypedExpr>(expr)) {
        VELOX_NYI("In Velox Plan, the aggregates type cannot be CallTypedExpr");
      } else {
        aggFunction->add_arguments()->mutable_value()->MergeFrom(
            exprConvertor_->toSubstraitExpr(arena, expr, inputType));
      }
    }

    // Set substrait aggregate Function reference and output type.
    if (functionMap_.find(funName) != functionMap_.end()) {
      aggFunction->set_function_reference(functionMap_[funName]);
    } else {
      VELOX_NYI("Couldn't find the aggregate function '{}' ", funName);
    }

    aggFunction->mutable_output_type()->MergeFrom(
        typeConvertor_->toSubstraitType(arena, aggregatesExpr->type()));

    // Set substrait aggregate Function phase.
    aggFunction->set_phase(toAggregationPhase(aggregateNode->step()));
  }

  // Direct output.
  aggregateRel->mutable_common()->mutable_direct();
}

void VeloxToSubstraitPlanConvertor::constructFunctionMap() {
  // TODO: Fetch all functions from velox's registry.

  functionMap_["plus"] = 0;
  functionMap_["multiply"] = 1;
  functionMap_["lt"] = 2;
  functionMap_["divide"] = 3;
  functionMap_["count"] = 4;
  functionMap_["sum"] = 5;
  functionMap_["mod"] = 6;
  functionMap_["eq"] = 7;
}

::substrait::Plan& VeloxToSubstraitPlanConvertor::addExtensionFunc(
    google::protobuf::Arena& arena) {
  // TODO: Fetch all functions from velox's registry and add them into substrait
  // extensions.
  // Now we just work around this part and add one function as dummy version to
  // pass filter and project round-trip test.
  auto substraitPlan =
      google::protobuf::Arena::CreateMessage<::substrait::Plan>(&arena);

  auto extensionFunction =
      substraitPlan->add_extensions()->mutable_extension_function();

  extensionFunction->set_extension_uri_reference(0);
  extensionFunction->set_function_anchor(0);
  extensionFunction->set_name("add:opt_i32_i32");

  extensionFunction =
      substraitPlan->add_extensions()->mutable_extension_function();
  extensionFunction->set_extension_uri_reference(0);
  extensionFunction->set_function_anchor(1);
  extensionFunction->set_name("multiply:opt_i32_i32");

  extensionFunction =
      substraitPlan->add_extensions()->mutable_extension_function();
  extensionFunction->set_extension_uri_reference(1);
  extensionFunction->set_function_anchor(2);
  extensionFunction->set_name("lt:i32_i32");

  extensionFunction =
      substraitPlan->add_extensions()->mutable_extension_function();
  extensionFunction->set_extension_uri_reference(0);
  extensionFunction->set_function_anchor(3);
  extensionFunction->set_name("divide:i32_i32");

  extensionFunction =
      substraitPlan->add_extensions()->mutable_extension_function();
  extensionFunction->set_extension_uri_reference(0);
  extensionFunction->set_function_anchor(4);
  extensionFunction->set_name("count:opt_i32");

  extensionFunction =
      substraitPlan->add_extensions()->mutable_extension_function();
  extensionFunction->set_extension_uri_reference(0);
  extensionFunction->set_function_anchor(5);
  extensionFunction->set_name("sum:opt_i32");

  extensionFunction =
      substraitPlan->add_extensions()->mutable_extension_function();
  extensionFunction->set_extension_uri_reference(0);
  extensionFunction->set_function_anchor(6);
  extensionFunction->set_name("modulus:i32_i32");

  extensionFunction =
      substraitPlan->add_extensions()->mutable_extension_function();
  extensionFunction->set_extension_uri_reference(0);
  extensionFunction->set_function_anchor(7);
  extensionFunction->set_name("equal:i64_i64");

  return *substraitPlan;
}

} // namespace facebook::velox::substrait
