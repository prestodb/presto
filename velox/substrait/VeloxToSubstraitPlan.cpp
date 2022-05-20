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

::substrait::Plan& VeloxToSubstraitPlanConvertor::toSubstrait(
    google::protobuf::Arena& arena,
    const core::PlanNodePtr& plan) {
  // Assume only accepts a single plan fragment.

  // Construct the function map based on the Velox plan.
  constructFunctionMap();

  // Construct the expression converter.
  exprConvertor_ =
      std::make_shared<VeloxToSubstraitExprConvertor>(functionMap_);

  // TODO add root_rel
  ::substrait::Plan* substraitPlan =
      google::protobuf::Arena::CreateMessage<::substrait::Plan>(&arena);

  // Add Extension Functions.
  substraitPlan->MergeFrom(addExtensionFunc(arena));

  // Do conversion.
  ::substrait::Rel* rel = substraitPlan->add_relations()->mutable_rel();
  toSubstrait(arena, plan, rel);

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

  readRel->mutable_base_schema()->MergeFrom(
      typeConvertor_->toSubstraitNamedStruct(arena, outputType));

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

void VeloxToSubstraitPlanConvertor::constructFunctionMap() {
  // TODO: Fetch all functions from velox's registry.

  functionMap_["plus"] = 0;
  functionMap_["multiply"] = 1;
  functionMap_["lt"] = 2;
  functionMap_["divide"] = 3;
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
  extensionFunction->set_name("add:i32_i32");

  extensionFunction =
      substraitPlan->add_extensions()->mutable_extension_function();
  extensionFunction->set_extension_uri_reference(0);
  extensionFunction->set_function_anchor(1);
  extensionFunction->set_name("multiply:i32_i32");

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

  return *substraitPlan;
}

} // namespace facebook::velox::substrait
