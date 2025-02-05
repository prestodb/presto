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

#include "velox/exec/FilterProject.h"
#include "velox/experimental/wave/exec/Project.h"
#include "velox/experimental/wave/exec/TableScan.h"
#include "velox/experimental/wave/exec/ToWave.h"
#include "velox/experimental/wave/exec/Values.h"
#include "velox/experimental/wave/exec/WaveDriver.h"
#include "velox/expression/ConstantExpr.h"
#include "velox/expression/FieldReference.h"
#include "velox/expression/ScopedVarSetter.h"

DEFINE_int32(ld_cost, 10, "Cost of load from memory");
DEFINE_int32(st_cost, 40, "Cost of store to memory");

namespace facebook::velox::wave {

using common::Subfield;
using exec::Expr;

std::string CodePosition::toString() const {
  if (empty()) {
    return "empty";
  }
  return fmt::format("<K:{}, S:{}, B:{}>", kernelSeq, step, branchIdx);
}

std::string OperandFlags::toString() const {
  return fmt::format(
      "{{flags: def={} first={} last={} wrap={} store={}}}",
      definedIn.toString(),
      firstUse.toString(),
      lastUse.toString(),
      wrappedAt,
      needStore);
}

void TableScanStep::visitResults(
    std::function<void(AbstractOperand*)> visitor) const {
  for (auto& out : results) {
    visitor(out);
  }
}

void ValuesStep::visitResults(
    std::function<void(AbstractOperand*)> visitor) const {
  for (auto& out : results) {
    visitor(out);
  }
}

void Compute::visitReferences(
    std::function<void(AbstractOperand*)> visitor) const {
  for (auto& in : operand->inputs) {
    visitor(in);
  }
}

void Compute::visitResults(
    std::function<void(AbstractOperand*)> visitor) const {
  visitor(operand);
}

void AggregateProbe::visitReferences(
    std::function<void(AbstractOperand*)> visitor) const {
  for (auto& key : keys) {
    visitor(key);
  }
}

void AggregateUpdate::visitReferences(
    std::function<void(AbstractOperand*)> visitor) const {
  for (auto& arg : args) {
    visitor(arg);
  }
  if (condition) {
    visitor(condition);
  }
}

void ReadAggregation::visitResults(
    std::function<void(AbstractOperand*)> visitor) const {
  for (auto& key : keys) {
    visitor(key);
  }
  for (auto& func : funcs) {
    visitor(func->result);
  }
}

AbstractOperand* markUse(AbstractOperand* op) {
  ++op->numUses;
  return op;
}

AbstractOperand* Scope::findValue(const Value& value) {
  auto it = operandMap.find(value);
  if (it == operandMap.end()) {
    if (parent) {
      return parent->findValue(value);
    }
    return nullptr;
  }
  return it->second;
}

std::string Scope::toString() const {
  std::stringstream out;
  for (auto& pair : operandMap) {
    out << pair.first.toString() << " = " << pair.second->toString() << "\n";
  }
  return out.str();
}

AbstractOperand* CompileState::fieldToOperand(Subfield& field, Scope* scope) {
  VELOX_CHECK(!namesResolved_);
  auto* op = scope->findValue(Value(&field));
  if (op) {
    return markUse(op);
  }
  auto* name =
      &reinterpret_cast<common::Subfield::NestedField*>(field.path()[0].get())
           ->name();
  VELOX_CHECK_EQ(topScopes_.size(), renames_.size());
  for (int32_t i = renames_.size() - 1; i >= 0; --i) {
    auto* op = topScopes_[i].findValue(Value(&field));
    if (op) {
      return markUse(op);
    }
    auto it = renames_[i].find(*name);
    if (it == renames_[i].end()) {
      VELOX_FAIL("Can't resolve {}", *name);
    }
    name = &it->second;
    auto* temp = toSubfield(*name);
    auto* def = topScopes_[i].findValue(Value(temp));
    if (def) {
      return markUse(def);
    }
  }
  VELOX_FAIL("Unresolved {}", *name);
}

AbstractOperand* CompileState::fieldToOperand(
    const core::FieldAccessTypedExpr& field,
    Scope* scope) {
  Subfield* subfield = toSubfield(field.name());
  return fieldToOperand(*subfield, scope);
}

std::vector<AbstractOperand*> CompileState::rowTypeToOperands(
    const RowTypePtr& rowType,
    DefinesMap* defines) {
  std::vector<AbstractOperand*> ops;
  for (auto i = 0; i < rowType->size(); ++i) {
    auto* field = toSubfield(rowType->nameOf(i));
    ops.push_back(fieldToOperand(*field, &topScope_));
    if (defines != nullptr) {
      (*defines)[Value(field)] = ops.back();
    }
  }
  return ops;
}

AbstractOperand* CompileState::switchOperand(
    const exec::SwitchExpr& switchExpr,
    Scope* scope) {
  auto& inputs = switchExpr.inputs();
  std::vector<AbstractOperand*> opInputs;
  Scope clauseScope(scope);
  for (auto i = 0; i < inputs.size(); i += 2) {
    opInputs.push_back(exprToOperand(*inputs[i], &clauseScope));
    if (i + 1 < inputs.size()) {
      opInputs.push_back(exprToOperand(*inputs[i + 1], &clauseScope));
    }
    clauseScope.operandMap.clear();
  }
  auto result = newOperand(switchExpr.type(), "r");
  result->expr = &switchExpr;
  result->inputs = std::move(opInputs);
  scope->operandMap[Value(&switchExpr)] = result;
  return result;
}

bool functionRetriable(const Expr& expr) {
  if (expr.name() == "CONCAT") {
    return true;
  }
  return false;
}

int32_t functionCost(const Expr& expr) {
  // Arithmetic
  return 1;
}

AbstractOperand* CompileState::exprToOperand(const Expr& expr, Scope* scope) {
  auto value = toValue(expr);
  auto op = scope->findValue(value);
  if (op) {
    return op;
  }
  if (auto* field = dynamic_cast<const exec::FieldReference*>(&expr)) {
    auto subfield = toSubfield(field->name());
    auto result = fieldToOperand(*subfield, scope);
    if (result) {
      return result;
    }
    VELOX_FAIL("Should have been defined");
  } else if (auto* constant = dynamic_cast<const exec::ConstantExpr*>(&expr)) {
    auto op = newOperand(constant->value()->type(), constant->toString());
    op->constant = constant->value();
    if (constant->value()->isNullAt(0)) {
      op->literalNull = true;
    } else {
      op->notNull = true;
    }
    return op;
  } else if (auto special = dynamic_cast<const exec::SpecialForm*>(&expr)) {
    if (auto* switchExpr = dynamic_cast<const exec::SwitchExpr*>(special)) {
      return switchOperand(*switchExpr, scope);
    }
    VELOX_UNSUPPORTED("No special forms: {}", expr.toString(1));
  }
  std::vector<AbstractOperand*> inputs;
  int32_t totalCost = 0;
  for (auto& in : expr.inputs()) {
    inputs.push_back(exprToOperand(*in, scope));

    totalCost += inputs.back()->costWithChildren;
  }
  auto result = newOperand(expr.type(), "r");
  result->retriable = functionRetriable(expr);
  result->expr = &expr;
  result->cost = functionCost(expr);
  result->costWithChildren = totalCost + result->cost;
  result->inputs = std::move(inputs);
  scope->operandMap[value] = result;
  return result;
}

Segment& CompileState::addSegment(
    BoundaryType boundary,
    const core::PlanNode* node,
    RowTypePtr outputType) {
  segments_.emplace_back();
  auto& last = segments_.back();
  last.ordinal = segments_.size() - 1;
  last.boundary = boundary;
  last.planNode = node;
  if (outputType && boundary == BoundaryType::kSource) {
    int32_t size = outputType->size();
    for (auto i = 0; i < size; ++i) {
      auto* subfield = toSubfield(outputType->nameOf(i));
      Value value(subfield);
      auto* op = newOperand(outputType->childAt(i), outputType->nameOf(i));
      op->definingSegment = last.ordinal;
      op->sourceNullable = boundary == BoundaryType::kSource;
      op->needsStore = boundary == BoundaryType::kSource;
      topScope_.operandMap[value] = op;
      last.topLevelDefined.push_back(op);
    }
  }
  last.outputType = outputType;
  return last;
}

void CompileState::tryFilter(const Expr& expr, const RowTypePtr& outputType) {
  auto& last = addSegment(BoundaryType::kExpr, nullptr, nullptr);
  last.topLevelDefined.push_back(exprToOperand(expr, &topScope_));
}

std::vector<AbstractOperand*> CompileState::tryExprSet(
    const exec::ExprSet& exprSet,
    int32_t begin,
    int32_t end,
    const std::vector<exec::IdentityProjection>* resultProjections,
    const RowTypePtr& outputType) {
  auto& exprs = exprSet.exprs();
  std::vector<AbstractOperand*> result;
  std::vector<Subfield*> resultSubfield;
  for (auto i = begin; i < end; ++i) {
    result.push_back(exprToOperand(*exprs[i], &topScope_));
    int32_t outputIdx = -1;
    for (auto& projection : *resultProjections) {
      if (projection.inputChannel == i) {
        outputIdx = projection.outputChannel;
        break;
      }
    }
    VELOX_CHECK_NE(-1, outputIdx);
    auto* subfield = toSubfield(outputType->nameOf(outputIdx));
    resultSubfield.push_back(subfield);
  }
  for (auto i = 0; i < result.size(); ++i) {
    topScope_.operandMap[Value(resultSubfield[i])] = result[i];
    segments_.back().projectedName.push_back(resultSubfield[i]);
    segments_.back().topLevelDefined.push_back(result[i]);
  }
  return result;
}

std::unordered_map<std::string, std::string> makeRenames(
    const std::vector<exec::IdentityProjection>& identities,
    const RowTypePtr inputType,
    const RowTypePtr& outputType) {
  std::unordered_map<std::string, std::string> map;
  for (auto p : identities) {
    map[outputType->nameOf(p.outputChannel)] =
        inputType->nameOf(p.inputChannel);
  }
  return map;
}

void CompileState::tryFilterProject(
    exec::Operator* op,
    RowTypePtr& outputType,
    int32_t& nodeIndex) {
  auto inputType = outputType;
  auto filterProject = reinterpret_cast<exec::FilterProject*>(op);
  outputType = driverFactory_.planNodes[nodeIndex]->outputType();
  auto data = filterProject->exprsAndProjection();
  auto& identityProjections = filterProject->identityProjections();
  int32_t firstProjection = 0;
  if (data.hasFilter) {
    tryFilter(*data.exprs->exprs()[0], outputType);
    auto filterOp = segments_.back().topLevelDefined[0];
    addSegment(BoundaryType::kFilter, nullptr, outputType);
    auto filterStep = makeStep<Filter>();
    filterStep->flag = filterOp;
    filterStep->nthWrap = wrapId_++;
    filterStep->indices = newOperand(INTEGER(), "indices");
    filterStep->indices->notNull = true;

    segments_.back().steps.push_back(filterStep);
    // If no projections, filter only. Done. Else take the output type
    // from the project node that follows and place the exprs.
    if (data.resultProjections->empty()) {
      return;
    }
    firstProjection = 1;
    ++nodeIndex;
    outputType = driverFactory_.planNodes[nodeIndex]->outputType();
    segments_.back().outputType = outputType;
  } else {
    addSegment(BoundaryType::kExpr, nullptr, nullptr);
  }

  auto operands = tryExprSet(
      *data.exprs,
      firstProjection,
      data.exprs->exprs().size(),
      data.resultProjections,
      outputType);
  renames_.push_back(makeRenames(identityProjections, inputType, outputType));
  topScopes_.push_back(std::move(topScope_));
}

bool CompileState::tryPlanOperator(
    exec::Operator* op,
    int32_t& nodeIndex,
    RowTypePtr& outputType) {
  auto& name = op->operatorType();
  if (name == "Values" || name == "TableScan") {
    auto node = driverFactory_.planNodes[nodeIndex];
    outputType = driverFactory_.planNodes[nodeIndex]->outputType();
    addSegment(BoundaryType::kSource, node.get(), outputType);
    if (name == "TableScan") {
      auto step = makeStep<TableScanStep>();
      step->node = dynamic_cast<const core::TableScanNode*>(node.get());
      step->results = rowTypeToOperands(node->outputType(), &step->defines);
      segments_.back().steps.push_back(step);
    } else {
      auto step = makeStep<ValuesStep>();
      step->node = dynamic_cast<const core::ValuesNode*>(node.get());
      step->results = rowTypeToOperands(node->outputType());
      segments_.back().steps.push_back(step);
    }
  } else if (name == "FilterProject") {
    tryFilterProject(op, outputType, nodeIndex);
  } else if (name == "Aggregation") {
    auto* node = dynamic_cast<const core::AggregationNode*>(
        driverFactory_.planNodes[nodeIndex].get());
    VELOX_CHECK_NOT_NULL(node);
    addSegment(BoundaryType::kAggregation, node, nullptr);
    auto step = makeStep<AggregateProbe>();
    auto* state = newState(StateKind::kGroupBy, node->id(), "");
    auto aggregationStep = node->step();
    step->state = state;
    step->id = ++aggCounter_;
    step->rows = newOperand(BIGINT(), "rows");
    step->continueLabelN = ++nextContinueLabel_;
    std::vector<AbstractOperand*> aggResults;
    for (auto& key : node->groupingKeys()) {
      step->keys.push_back(fieldToOperand(*key, &topScope_));
    }
    std::vector<const AggregateUpdate*> allUpdates;
    auto& output = node->outputType();
    for (auto i = 0; i < node->aggregates().size(); ++i) {
      auto& agg = node->aggregates()[i];
      std::vector<AbstractOperand*> args;
      for (auto& expr : agg.call->inputs()) {
        if (auto fieldAccess =
                std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(
                    expr)) {
          args.push_back(fieldToOperand(*fieldAccess, &topScope_));
        } else if (
            auto literal =
                std::dynamic_pointer_cast<const core::ConstantTypedExpr>(
                    expr)) {
          auto expr = std::make_shared<exec::ConstantExpr>(
              literal->toConstantVector(pool_));
          args.push_back(exprToOperand(*expr, &topScope_));
        } else {
          VELOX_FAIL("Bad arg to aggregation");
        }
      }
      auto* func = makeStep<AggregateUpdate>();
      func->step = aggregationStep;
      func->name = agg.call->name();
      func->accumulatorIdx = i;
      func->rows = step->rows;
      func->signature = agg.rawInputTypes;
      func->generator = aggregateRegistry().getGenerator(*func);
      func->args = std::move(args);
      allUpdates.push_back(func);
    }
    step->updates = allUpdates;
    segments_.back().steps.push_back(step);
    outputType = node->outputType();
    addSegment(BoundaryType::kSource, node, outputType);
    auto read = makeStep<ReadAggregation>();
    read->probe = step;
    read->state = state;
    read->continueLabelN = ++nextContinueLabel_;

    for (auto i = 0; i < node->groupingKeys().size(); ++i) {
      read->keys.push_back(
          fieldToOperand(*toSubfield(outputType->nameOf(i)), &topScope_));
    }
    read->funcs = std::move(allUpdates);
    for (auto i = 0; i < read->funcs.size(); ++i) {
      const_cast<AggregateUpdate*>(read->funcs[i])->result = fieldToOperand(
          *toSubfield(output->nameOf(i + read->keys.size())), &topScope_);
    }
    segments_.back().steps.push_back(read);
  } else {
    return false;
  }
  return true;
}

bool CompileState::makeSegments(int32_t& operatorIndex) {
  auto operators = driver_.operators();
  int32_t nodeIndex = 0;
  RowTypePtr outputType;
  RowTypePtr inputType;
  for (; operatorIndex < operators.size(); ++operatorIndex) {
    if (!tryPlanOperator(operators[operatorIndex], nodeIndex, outputType)) {
      break;
    }
    ++nodeIndex;
  }
  if (!segments_.back().outputType) {
    segments_.back().outputType = outputType;
  }
  for (auto i = 0; i < outputType->size(); ++i) {
    auto* result =
        fieldToOperand(*toSubfield(outputType->nameOf(i)), &topScope_);
    // Returned to host, must be in memory.
    result->needsStore = true;
  }
  return true;
}

int32_t countLoads(PipelineCandidate& candidate, AbstractOperand* op) {
  int32_t count = 0;
  auto& f = candidate.flags(op);
  if (f.needStore) {
    return 1;
  }
  for (auto* in : op->inputs) {
    count += countLoads(candidate, in);
  }
  return count;
}

bool isInlinable(PipelineCandidate& candidate, AbstractOperand* op) {
  auto& flags = candidate.flags(op);
  if (flags.needStore) {
    return true;
  }
  int32_t numLoads = countLoads(candidate, op);
  if (op->numUses < 2) {
    return true;
  }
  return numLoads * op->numUses < 5;
}

void recordReference(PipelineCandidate& candidate, AbstractOperand* op) {
  auto& flags = candidate.flags(op);
  if (flags.firstUse.empty()) {
    flags.firstUse = CodePosition(
        candidate.steps.size() - 1,
        candidate.boxIdx,
        candidate.currentBox->steps.size());
  }
  if (flags.wrappedAt == AbstractOperand::kNoWrap) {
    bool first = true;
    for (auto seq = flags.definedIn.kernelSeq; seq < candidate.steps.size();
         ++seq) {
      auto branch = first ? flags.definedIn.branchIdx : 0;
      auto* box = &candidate.steps[seq][branch];
      if (!first) {
        flags.needStore = true;
        if (candidate.steps[seq].size() > 1) {
          // if multiple parallel kernel boxes, no cardinality change.
          continue;
        }
      }
      for (auto i = first ? flags.definedIn.step + 1 : 0; i < box->steps.size();
           ++i) {
        auto nthWrap = box->steps[i]->isWrap();
        if (nthWrap != AbstractOperand::kNoWrap) {
          op->wrappedAt = nthWrap;
          flags.wrappedAt = nthWrap;
          break;
        }
      }
      first = false;
    }
  }
  flags.lastUse = CodePosition(
      candidate.steps.size() - 1,
      candidate.boxIdx,
      candidate.currentBox->steps.size());
}

void distinctLeavesInner(
    PipelineCandidate& candidate,
    AbstractOperand* op,
    folly::F14FastSet<AbstractOperand*>& ops) {
  if (op->constant) {
    return;
  }
  if (ops.count(op)) {
    return;
  }
  if (op->inputs.empty()) {
    ops.insert(op);
    return;
  }
  auto flags = candidate.flags(op);
  if (!flags.definedIn.empty()) {
    // If a subexpr is already placed, use the nullness of that instead of the
    // nullness of its leaves.
    ops.insert(op);
    return;
  }
  for (auto& input : op->inputs) {
    distinctLeavesInner(candidate, input, ops);
  }
}

std::vector<AbstractOperand*> distinctLeaves(
    PipelineCandidate& candidate,
    AbstractOperand* op) {
  std::vector<AbstractOperand*> result;
  folly::F14FastSet<AbstractOperand*> ops;
  distinctLeavesInner(candidate, op, ops);
  for (auto& op : ops) {
    result.push_back(op);
  }
  return result;
}

NullCheck* CompileState::addNullCheck(
    PipelineCandidate& candidate,
    AbstractOperand* op) {
  auto* check = makeStep<NullCheck>();
  check->operands = distinctLeaves(candidate, op);
  check->label = ++labelCounter_;
  check->result = op;
  return check;
}
bool shouldDelay(const AbstractOperand* op, const OperandFlags& flags) {
  auto* expr = op->expr;
  if (!expr) {
    return false;
  }
  if (functionRetriable(*expr)) {
    return false;
  }
  auto& fields = expr->distinctFields();
  int32_t expensive = flags.inInlineGroupBy ? 5 : 20;
  if (op->costWithChildren >= expensive) {
    return false;
  }
  if (op->numUses > 1 && fields.size() > 1) {
    return false;
  }
  return true;
}

void CompileState::placeExpr(
    PipelineCandidate& candidate,
    AbstractOperand* op,
    bool mayDelay) {
  if (op->constant) {
    return;
  }
  auto& flags = candidate.flags(op);
  if (!flags.definedIn.empty()) {
    recordReference(candidate, op);
  } else {
    if (mayDelay && shouldDelay(op, flags)) {
      return;
    }
    bool checkNulls = !insideNullPropagating_ && op->expr->propagatesNulls();
    ScopedVarSetter s(&insideNullPropagating_, true, checkNulls);
    NullCheck* check;
    if (checkNulls) {
      check = addNullCheck(candidate, op);
      candidate.currentBox->steps.push_back(check);
    }
    for (auto* in : op->inputs) {
      placeExpr(candidate, in, false);
    }
    flags.definedIn = CodePosition(
        candidate.steps.size() - 1,
        candidate.boxIdx,
        candidate.currentBox->steps.size());
    auto inst = makeStep<Compute>();
    inst->operand = op;
    candidate.currentBox->steps.push_back(inst);
    if (checkNulls) {
      auto end = makeStep<EndNullCheck>();
      check->endIdx = candidate.currentBox->steps.size();
      end->result = op;
      end->label = check->label;
      candidate.currentBox->steps.push_back(end);
    }
  }
}

void CompileState::markOutputStored(
    PipelineCandidate& candidate,
    Segment& segment) {
  auto& defined = segment.topLevelDefined;
  for (auto i = 0; i < defined.size(); ++i) {
    auto* op = defined[i];
    candidate.flags(op).needStore = true;
  }
}

void newKernel(PipelineCandidate& candidate) {
  candidate.steps.emplace_back();
  candidate.steps.back().emplace_back();
  candidate.currentBox = &candidate.steps.back()[0];
  candidate.boxIdx = 0;
}

bool isSink(const PipelineCandidate& candidate) {
  auto& level = candidate.steps.back();
  bool result;
  for (auto i = 0; i < level.size(); ++i) {
    auto& box = level[i];
    bool sink = !box.steps.empty() && box.steps.back()->isSink();
    if (i == 0) {
      result = sink;
    } else {
      VELOX_CHECK_EQ(
          result, sink, "All levels must be either sink or not sink");
    }
  }
  return result;
}

void CompileState::recordCandidate(
    PipelineCandidate& candidate,
    int32_t lastSegmentIdx) {
  auto& segment = segments_[lastSegmentIdx];
  candidate.outputType = segment.outputType;
  // Mark store needed for output operands if the segment does not end with a
  // sink.
  if (!isSink(candidate)) {
    for (auto i = 0; i < segment.topLevelDefined.size(); ++i) {
      auto* op = segment.topLevelDefined[i];
      auto& flags = candidate.flags(op);
      flags.needStore = true;
    }
  }
  candidates_.push_back(std::move(candidate));
}

void CompileState::placeAggregation(
    PipelineCandidate& candidate,
    Segment& segment) {
  // Sets the inlined updates to be all updates. An alternative is to spread the
  // updates into the next kernel with different accumulators done on different
  // TBs.
  for (auto& step : segment.steps) {
    if (step->kind() == StepKind::kAggregateProbe) {
      auto& probe = step->as<AggregateProbe>();
      probe.allUpdatesInlined = true;
      for (auto& key : probe.keys) {
        placeExpr(candidate, key, false);
      }
      candidate.currentBox->steps.push_back(&probe);
      auto firstUpdateIdx = candidate.currentBox->steps.size();
      for (auto& update : probe.updates) {
        if (update->condition) {
          placeExpr(candidate, update->condition, false);
        }
        for (auto& arg : update->args) {
          placeExpr(candidate, arg, false);
        }
        candidate.currentBox->steps.push_back(
            const_cast<AggregateUpdate*>(update));
      }
      // Move the kernel steps for updates into 'inlinedUpdates' of the probe.
      probe.inlinedUpdates.insert(
          probe.inlinedUpdates.end(),
          candidate.currentBox->steps.begin() + firstUpdateIdx,
          candidate.currentBox->steps.end());
      candidate.currentBox->steps.resize(firstUpdateIdx);
      break;
    }
  }
}
bool CompileState::hasSink(int32_t idx) {
  for (auto i = idx; i < segments_.size(); ++i) {
    auto bound = segments_[i].boundary;
    if (bound == BoundaryType::kAggregation) {
      return true;
    }
  }
  return false;
}

void CompileState::planSegment(
    PipelineCandidate& candidate,
    float inputBatch,
    int32_t segmentIdx) {
  auto& segment = segments_[segmentIdx];
  switch (segment.boundary) {
    case BoundaryType::kSource: {
      if (candidate.steps.size() > 1 || !candidate.currentBox->steps.empty()) {
        // A pipeline barrier.
        recordCandidate(candidate, segmentIdx - 1);
        return;
      }
      bool needNewKernel = false;
      auto* node = segment.planNode;
      if (auto* scan = dynamic_cast<const core::TableScanNode*>(node)) {
        candidate.currentBox->steps.push_back(segment.steps[0]);
        needNewKernel = true;
      } else if (auto* values = dynamic_cast<const core::ValuesNode*>(node)) {
        candidate.currentBox->steps.push_back(segment.steps[0]);
        needNewKernel = true;
      } else if (
          auto* read = dynamic_cast<const core::AggregationNode*>(node)) {
        auto* step = segment.steps[0];
        candidate.currentBox->steps.push_back(step);
      }
      VELOX_CHECK_LE(1, candidate.currentBox->steps.size());
      auto pos = CodePosition(0, 0, candidate.currentBox->steps.size() - 1);
      for (auto* op : segment.topLevelDefined) {
        auto& flags = candidate.flags(op);
        flags.definedIn = pos;
      }

      markOutputStored(candidate, segment);
      // If the source should be a standalone kernel, like Values or
      // TableScan and there is more to plan, add a kernel boundary.
      if (needNewKernel && segmentIdx < segments_.size() - 1) {
        newKernel(candidate);
      }
      break;
    }
    case BoundaryType::kExpr: {
      bool mayDelay = hasSink(segmentIdx);
      for (auto i = 0; i < segment.topLevelDefined.size(); ++i) {
        auto* op = segment.topLevelDefined[i];
        placeExpr(candidate, op, mayDelay);
      }
      break;
    }
    case BoundaryType::kFilter: {
      auto& filter = segment.steps[0]->as<Filter>();
      placeExpr(candidate, filter.flag, false);
      candidate.currentBox->steps.push_back(&filter);
      bool mayDelay = hasSink(segmentIdx);
      for (auto i = 0; i < segment.topLevelDefined.size(); ++i) {
        placeExpr(candidate, segment.topLevelDefined[i], mayDelay);
      }
      break;
    }
    case BoundaryType::kAggregation: {
      // If there are many parallel column groups, bring them to one.
      if (candidate.steps.back().size() > 1) {
        newKernel(candidate);
      }
      // Append the aggregate probe and updates. May inline all or have a wider
      // kernel for updates if many updates and few top level rows.
      placeAggregation(candidate, segment);
      break;
    }
    default:
      VELOX_NYI();
  }
  if (segmentIdx == segments_.size() - 1) {
    recordCandidate(candidate, segmentIdx);
    return;
  }

  planSegment(candidate, inputBatch, segmentIdx + 1);
}

void CompileState::pickBest() {
  // There is only one candidate. Pick that.
  int32_t selectedIdx = 0;
  selectedPipelines_.push_back(std::move(candidates_[selectedIdx]));
  candidates_.clear();
}

void PipelineCandidate::markParams(
    KernelBox& box,
    int32_t kernelSeq,
    int32_t branchIdx,
    std::vector<LevelParams>& params) {
  for (auto stepIdx = 0; stepIdx < box.steps.size(); ++stepIdx) {
    auto referenceVisitor = [&](AbstractOperand* op) {
      if (op->constant) {
        return;
      }
      auto& flags = this->flags(op);
      if (flags.definedIn.kernelSeq < kernelSeq) {
        levelParams[kernelSeq].input.add(op->id);
      }
    };
    auto resultVisitor = [&](AbstractOperand* op) {
      auto& flags = this->flags(op);
      if (flags.definedIn.empty()) {
        flags.definedIn = CodePosition(kernelSeq, branchIdx, stepIdx);
      }
      // If used later or used in wrap (filter indices) the op goes to output.
      if (flags.lastUse.kernelSeq > kernelSeq ||
          box.steps[stepIdx]->kind() == StepKind::kFilter) {
        levelParams[kernelSeq].output.add(op->id);
      } else {
        levelParams[kernelSeq].local.add(op->id);
      }
    };
    auto step = box.steps[stepIdx];
    step->visitReferences(referenceVisitor);
    step->visitResults(resultVisitor);
    if (step->kind() == StepKind::kAggregateProbe) {
      auto probe = step->as<AggregateProbe>();
      for (auto j = 0; j < probe.inlinedUpdates.size(); ++j) {
        probe.inlinedUpdates[j]->visitReferences(referenceVisitor);
        probe.inlinedUpdates[j]->visitResults(resultVisitor);
      }
    }
    box.steps[stepIdx]->visitStates([&](AbstractState* state) {
      levelParams[kernelSeq].states.add(state->id);
    });
  }
}

void PipelineCandidate::makeOperandSets(int32_t pipelineSeq) {
  levelParams.resize(steps.size());
  for (auto kernelSeq = 0; kernelSeq < steps.size(); ++kernelSeq) {
    for (auto i = 0; i < steps[kernelSeq].size(); ++i) {
      markParams(steps[kernelSeq][i], kernelSeq, i, levelParams);
    }
  }
}

void CompileState::markHostOutput() {
  VELOX_CHECK_NOT_NULL(resultOrder_);
  auto& candidate = selectedPipelines_.back();
  CodePosition afterEnd(candidate.steps.size());
  for (auto i = 0; i < resultOrder_->size(); ++i) {
    auto* op = operandById((*resultOrder_)[i]);
    auto& flags = candidate.flags(op);
    flags.lastUse = afterEnd;
    flags.needStore = true;
  }
}

void CompileState::planPipelines() {
  int32_t startIdx = 0;
  for (;;) {
    PipelineCandidate candidate;
    newKernel(candidate);
    planSegment(candidate, 100000, startIdx);
    pickBest();
    bool found = false;
    for (auto i = startIdx + 1; i < segments_.size(); ++i) {
      if (segments_[i].boundary == BoundaryType::kSource) {
        startIdx = i;
        found = true;
        break;
      }
    }
    if (!found) {
      break;
    }
  }
  for (pipelineIdx_ = 0; pipelineIdx_ < selectedPipelines_.size();
       ++pipelineIdx_) {
    // Mark the operands to return to host as referenced in a fictitious step
    // after the last. This makes them outputs of the producing level/operator.
    if (pipelineIdx_ == selectedPipelines_.size() - 1) {
      markHostOutput();
    }
    selectedPipelines_[pipelineIdx_].makeOperandSets(pipelineIdx_);
  }
}

RowTypePtr CompileState::makeOperators(
    int32_t& operatorIndex,
    std::vector<OperandId>& resultOrder) {
  makeSegments(operatorIndex);
  auto outputType = segments_.back().outputType;
  for (auto i = 0; i < outputType->size(); ++i) {
    auto op = fieldToOperand(*toSubfield(outputType->nameOf(i)), &topScope_);
    resultOrder.push_back(op->id);
  }
  resultOrder_ = &resultOrder;
  namesResolved_ = true;
  planPipelines();
  generatePrograms();
  resultOrder_ = nullptr;
  return outputType;
}

std::string CompileState::segmentString() const {
  std::stringstream out;
  for (auto i = 0; i < segments_.size(); ++i) {
    out << segments_[i].toString() << std::endl;
  }
  return out.str();
}

std::string Segment::toString() const {
  std::stringstream out;
  out << fmt::format("Segment {}: ", static_cast<int32_t>(boundary))
      << std::endl;
  for (auto i = 0; i < steps.size(); ++i) {
    out << i << ": " << steps[i]->toString() << std::endl;
  }
  out << std::endl << "Results:" << std::endl;
  for (auto i = 0; i < topLevelDefined.size(); ++i) {
    out << fmt::format(
               "{}: {} as {}",
               i,
               topLevelDefined[i]->toString(),
               projectedName.size() > i ? projectedName[i]->toString() : "-")
        << std::endl;
  }
  return out.str();
}

std::string PipelineCandidate::toString() const {
  std::stringstream out;
  for (auto kernelSeq = 0; kernelSeq < steps.size(); ++kernelSeq) {
    out << fmt::format(
               "Kernel {} branches={}:", kernelSeq, steps[kernelSeq].size())
        << std::endl;
    out << "  Input=" << levelParams[kernelSeq].input.toString() << std::endl
        << "  Local=" << levelParams[kernelSeq].local.toString() << std::endl
        << "  Output=" << levelParams[kernelSeq].output.toString() << std::endl;
    for (auto branchIdx = 0; branchIdx < steps[kernelSeq].size(); ++branchIdx) {
      auto& box = steps[kernelSeq][branchIdx];
      for (auto stepIdx = 0; stepIdx < box.steps.size(); ++stepIdx) {
        out << fmt::format("  {}: {}", stepIdx, box.steps[stepIdx]->toString())
            << std::endl;
      }
    }
  }
  return out.str();
}

} // namespace facebook::velox::wave
