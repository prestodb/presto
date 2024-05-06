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

#include "velox/experimental/wave/exec/ToWave.h"
#include "velox/exec/FilterProject.h"
#include "velox/experimental/wave/exec/Aggregation.h"
#include "velox/experimental/wave/exec/Project.h"
#include "velox/experimental/wave/exec/TableScan.h"
#include "velox/experimental/wave/exec/Values.h"
#include "velox/experimental/wave/exec/WaveDriver.h"
#include "velox/expression/ConstantExpr.h"
#include "velox/expression/FieldReference.h"

DEFINE_int64(velox_wave_arena_unit_size, 1 << 30, "Per Driver GPU memory size");

namespace facebook::velox::wave {

using exec::Expr;

common::Subfield* CompileState::toSubfield(const Expr& expr) {
  std::string name = expr.toString();
  return toSubfield(name);
}

common::Subfield* CompileState::toSubfield(const std::string& name) {
  auto it = subfields_.find(name);
  if (it == subfields_.end()) {
    auto field = std::make_unique<common::Subfield>(name);
    auto result = field.get();
    subfields_[name] = std::move(field);
    return result;
  }
  return it->second.get();
}

// true if expr translates to Subfield path.
bool isField(const Expr& expr) {
  if (auto* field = dynamic_cast<const exec::FieldReference*>(&expr)) {
    return (expr.inputs().empty());
  }
  return false;
}

Value CompileState::toValue(const Expr& expr) {
  if (isField(expr)) {
    auto* subfield = toSubfield(expr);
    return Value(subfield);
  }
  return Value(&expr);
}

AbstractOperand* CompileState::newOperand(AbstractOperand& other) {
  auto newOp = std::make_unique<AbstractOperand>(other, operandCounter_++);
  operands_.push_back(std::move(newOp));
  return operands_.back().get();
}

AbstractOperand* CompileState::newOperand(
    const TypePtr& type,
    const std::string& label) {
  operands_.push_back(
      std::make_unique<AbstractOperand>(operandCounter_++, type, label));
  auto op = operands_.back().get();
  return op;
}

AbstractOperand* CompileState::addIdentityProjections(AbstractOperand* source) {
  AbstractOperand* result = nullptr;

  int32_t latest = 0;
  auto it = operandOperatorIndex_.find(source);
  VELOX_CHECK(
      it != operandOperatorIndex_.end(),
      "The operand being projected through must b defined first");
  latest = it->second;
  result = source;
  for (auto i = latest; i < operators_.size(); ++i) {
    if (auto wrap = operators_[i]->findWrap()) {
      if (operators_[i]->isExpanding()) {
        auto newResult = newOperand(*result);
        wrap->addWrap(result, newResult);
        result = newResult;
      } else {
        wrap->addWrap(result);
      }
    }
  }
  return result;
}

AbstractOperand* CompileState::findCurrentValue(Value value) {
  auto it = projectedTo_.find(value);
  AbstractOperand* source;
  if (it == projectedTo_.end()) {
    auto originIt = definedBy_.find(value);
    if (originIt == definedBy_.end()) {
      return nullptr;
    }
    source = originIt->second;
    // The operand is defined earlier, so must get translated through
    // cardinality changes. Or if it is not defined earlier, it is defined in
    // the WaveOperator being constructed, in which case,i.e. the operand in
    // 'definedBy_'.
    auto projected = addIdentityProjections(source);
    return projected ? projected : originIt->second;
  }
  return it->second;
}

std::optional<OpCode> binaryOpCode(const Expr& expr) {
  auto& name = expr.name();
  if (name == "plus") {
    return OpCode::kPlus;
  }
  if (name == "lt") {
    return OpCode::kLT;
  }
  return std::nullopt;
}

Program* CompileState::newProgram() {
  auto program = std::make_shared<Program>();
  allPrograms_.push_back(program);
  return program.get();
}

Program* CompileState::programOf(AbstractOperand* op, bool create) {
  auto it = definedIn_.find(op);
  if (it == definedIn_.end()) {
    if (!create) {
      return nullptr;
    }
    return newProgram();
  }
  return it->second;
}

void CompileState::addInstruction(
    std::unique_ptr<AbstractInstruction> instruction,
    AbstractOperand* result,
    const std::vector<Program*>& inputs) {
  Program* common = nullptr;
  bool many = false;
  for (auto* program : inputs) {
    if (!program->isMutable()) {
      continue;
    }
    if (!common && program->isMutable()) {
      common = program;
    } else if (common == program) {
      continue;
    } else {
      many = true;
      break;
    }
  }
  Program* program;
  if (common && !many) {
    program = common;
  } else {
    program = newProgram();
  }
  for (auto source : inputs) {
    if (source != program) {
      program->addSource(source);
    }
  }
  program->add(std::move(instruction));
  definedIn_[result] = program;
}

bool maybeNotNull(const AbstractOperand* op) {
  if (!op) {
    return true;
  }
  return op->conditionalNonNull || op->notNull || op->sourceNullable;
}

void CompileState::addNullableIf(
    const AbstractOperand* op,
    std::vector<OperandId>& nullableIf) {
  for (auto id : op->nullableIf) {
    if (std::find(nullableIf.begin(), nullableIf.end(), id) ==
        nullableIf.end()) {
      nullableIf.push_back(id);
    }
  }
}

void CompileState::setConditionalNullable(AbstractBinary& binary) {
  if (maybeNotNull(binary.left) && maybeNotNull(binary.right)) {
    binary.result->conditionalNonNull = true;
    addNullableIf(binary.left, binary.result->nullableIf);
    addNullableIf(binary.right, binary.result->nullableIf);
  }
}

AbstractOperand* CompileState::addExpr(const Expr& expr) {
  auto value = toValue(expr);
  auto current = findCurrentValue(value);
  if (current) {
    return current;
  }

  if (auto* field = dynamic_cast<const exec::FieldReference*>(&expr)) {
    VELOX_FAIL("Should have been defined");
  } else if (auto* constant = dynamic_cast<const exec::ConstantExpr*>(&expr)) {
    if (predicate_) {
      auto result = newOperand(constant->type(), constant->toString());
      currentProgram_->add(std::make_unique<AbstractLiteral>(
          constant->value(), result, predicate_));
      return result;
    } else {
      auto op = newOperand(constant->value()->type(), constant->toString());
      op->constant = constant->value();
      if (constant->value()->isNullAt(0)) {
        op->literalNull = true;
      } else {
        op->notNull = true;
      }
      return op;
    }
  } else if (dynamic_cast<const exec::SpecialForm*>(&expr)) {
    VELOX_UNSUPPORTED("No special forms: {}", expr.toString(1));
  }
  auto opCode = binaryOpCode(expr);
  if (!opCode.has_value()) {
    VELOX_UNSUPPORTED("Expr not supported: {}", expr.toString());
  }
  auto result = newOperand(expr.type(), "r");
  auto leftOp = addExpr(*expr.inputs()[0]);
  auto rightOp = addExpr(*expr.inputs()[1]);
  auto instruction =
      std::make_unique<AbstractBinary>(opCode.value(), leftOp, rightOp, result);
  setConditionalNullable(*instruction);

  auto leftProgram = definedIn_[leftOp];
  auto rightProgram = definedIn_[rightOp];
  std::vector<Program*> sources;
  if (leftProgram) {
    sources.push_back(leftProgram);
  }
  if (rightProgram) {
    sources.push_back(rightProgram);
  }
  addInstruction(std::move(instruction), result, sources);
  return result;
}

std::vector<AbstractOperand*> CompileState::addExprSet(
    const exec::ExprSet& exprSet,
    int32_t begin,
    int32_t end) {
  auto& exprs = exprSet.exprs();
  std::vector<AbstractOperand*> result;
  for (auto i = begin; i < end; ++i) {
    result.push_back(addExpr(*exprs[i]));
    programOf(result.back())->addLabel(exprs[i]->toString(true));
  }
  return result;
}

std::vector<std::vector<ProgramPtr>> CompileState::makeLevels(
    int32_t startIndex) {
  std::vector<std::vector<ProgramPtr>> levels;
  folly::F14FastSet<Program*> toAdd;
  for (auto i = startIndex; i < allPrograms_.size(); ++i) {
    toAdd.insert(allPrograms_[i].get());
  }
  while (!toAdd.empty()) {
    std::vector<ProgramPtr> level;
    for (auto& program : toAdd) {
      auto& depends = program->dependsOn();
      auto independent = true;
      for (auto& d : depends) {
        if (toAdd.count(d)) {
          independent = false;
          break;
        }
      }
      if (independent) {
        level.push_back(program->shared_from_this());
      }
    }
    for (auto added : level) {
      toAdd.erase(added.get());
    }
    levels.push_back(std::move(level));
  }
  return levels;
}

int32_t findOutputChannel(
    const std::vector<exec::IdentityProjection>& projections,
    int32_t exprIndex) {
  for (auto& projection : projections) {
    if (projection.inputChannel == exprIndex) {
      return projection.outputChannel;
    }
  }
  VELOX_FAIL("Expr without output channel");
}

void CompileState::addFilter(const Expr& expr, const RowTypePtr& outputType) {
  int32_t numPrograms = allPrograms_.size();
  auto condition = addExpr(expr);
  auto indices = newOperand(INTEGER(), "indices");
  indices->notNull = true;
  auto program = programOf(condition);
  program->addLabel(expr.toString(true));
  program->markOutput(indices->id);
  program->add(std::make_unique<AbstractFilter>(condition, indices));
  auto wrapUnique = std::make_unique<AbstractWrap>(indices, wrapCounter_++);
  auto wrap = wrapUnique.get();
  program->add(std::move(wrapUnique));
  auto levels = makeLevels(numPrograms);
  operators_.push_back(std::make_unique<Project>(
      *this, outputType, std::vector<AbstractOperand*>{}, levels, wrap));
}

void CompileState::addFilterProject(
    exec::Operator* op,
    RowTypePtr& outputType,
    int32_t& nodeIndex) {
  auto filterProject = reinterpret_cast<exec::FilterProject*>(op);
  outputType = driverFactory_.planNodes[nodeIndex]->outputType();
  auto data = filterProject->exprsAndProjection();
  auto& identityProjections = filterProject->identityProjections();
  int32_t firstProjection = 0;
  if (data.hasFilter) {
    addFilter(*data.exprs->exprs()[0], outputType);
    firstProjection = 1;
    ++nodeIndex;
    outputType = driverFactory_.planNodes[nodeIndex]->outputType();
  }
  int32_t numPrograms = allPrograms_.size();
  auto operands =
      addExprSet(*data.exprs, firstProjection, data.exprs->exprs().size());
  std::vector<std::pair<Value, AbstractOperand*>> pairs;
  for (auto i = 0; i < operands.size(); ++i) {
    int32_t channel =
        findOutputChannel(*data.resultProjections, i + firstProjection);
    auto subfield = toSubfield(outputType->nameOf(channel));
    auto program = programOf(operands[i], false);
    if (program) {
      program->markOutput(operands[i]->id);
      definedIn_[operands[i]] = program;
    }
    Value value(subfield);
    definedBy_[value] = operands[i];
    pairs.push_back(std::make_pair(value, operands[i]));
  }
  auto levels = makeLevels(numPrograms);
  operators_.push_back(
      std::make_unique<Project>(*this, outputType, operands, levels));
  for (auto& [value, operand] : pairs) {
    operators_.back()->defined(value, operand);
  }
}

bool CompileState::reserveMemory() {
  if (arena_) {
    return true;
  }
  auto* allocator = getAllocator(getDevice());
  arena_ =
      std::make_unique<GpuArena>(FLAGS_velox_wave_arena_unit_size, allocator);
  return true;
}

const std::shared_ptr<aggregation::AggregateFunctionRegistry>&
CompileState::aggregateFunctionRegistry() {
  if (!aggregateFunctionRegistry_) {
    aggregateFunctionRegistry_ =
        std::make_shared<aggregation::AggregateFunctionRegistry>(
            getAllocator(getDevice()));
    Stream stream;
    aggregateFunctionRegistry_->addAllBuiltInFunctions(stream);
    stream.wait();
  }
  return aggregateFunctionRegistry_;
}

bool CompileState::addOperator(
    exec::Operator* op,
    int32_t& nodeIndex,
    RowTypePtr& outputType) {
  auto& name = op->operatorType();
  if (name == "Values") {
    if (!reserveMemory()) {
      return false;
    }
    operators_.push_back(std::make_unique<Values>(
        *this,
        *reinterpret_cast<const core::ValuesNode*>(
            driverFactory_.planNodes[nodeIndex].get())));
    outputType = driverFactory_.planNodes[nodeIndex]->outputType();
  } else if (name == "FilterProject") {
    if (!reserveMemory()) {
      return false;
    }
    addFilterProject(op, outputType, nodeIndex);
  } else if (name == "Aggregation") {
    if (!reserveMemory()) {
      return false;
    }
    auto* node = dynamic_cast<const core::AggregationNode*>(
        driverFactory_.planNodes[nodeIndex].get());
    VELOX_CHECK_NOT_NULL(node);
    operators_.push_back(std::make_unique<Aggregation>(
        *this, *node, aggregateFunctionRegistry()));
    outputType = node->outputType();
  } else if (name == "TableScan") {
    if (!reserveMemory()) {
      return false;
    }
    auto scan = reinterpret_cast<const core::TableScanNode*>(
        driverFactory_.planNodes[nodeIndex].get());
    outputType = driverFactory_.planNodes[nodeIndex]->outputType();

    operators_.push_back(
        std::make_unique<TableScan>(*this, operators_.size(), *scan));
    outputType = scan->outputType();
  } else {
    return false;
  }
  return true;
}

bool isProjectedThrough(
    const std::vector<exec::IdentityProjection>& projectedThrough,
    int32_t i,
    int32_t& inputChannel) {
  for (auto& projection : projectedThrough) {
    if (projection.outputChannel == i) {
      inputChannel = projection.inputChannel;
      return true;
    }
  }
  return false;
}

bool CompileState::compile() {
  auto operators = driver_.operators();
  auto& nodes = driverFactory_.planNodes;

  int32_t first = 0;
  int32_t operatorIndex = 0;
  int32_t nodeIndex = 0;
  RowTypePtr outputType;
  // Make sure operator states are initialized.  We will need to inspect some of
  // them during the transformation.
  driver_.initializeOperators();
  RowTypePtr inputType;
  for (; operatorIndex < operators.size(); ++operatorIndex) {
    int32_t previousNumOperators = operators_.size();
    auto& identity = operators[operatorIndex]->identityProjections();
    // The columns that are projected through are renamed. They may also get an
    // indirection after the new operator is placed.
    std::vector<std::pair<AbstractOperand*, int32_t>> identityProjected;
    for (auto& projection : identity) {
      identityProjected.push_back(std::make_pair(
          findCurrentValue(
              Value(toSubfield(inputType->nameOf(projection.inputChannel)))),
          projection.outputChannel));
    }
    if (!addOperator(operators[operatorIndex], nodeIndex, outputType)) {
      break;
    }
    ++nodeIndex;
    for (auto newIndex = previousNumOperators; newIndex < operators_.size();
         ++newIndex) {
      for (auto i = 0; i < outputType->size(); ++i) {
        auto& name = outputType->nameOf(i);
        Value value = Value(toSubfield(name));
        int32_t inputChannel;
        if (isProjectedThrough(identity, i, inputChannel)) {
          continue;
        }
        auto operand = operators_[newIndex]->defines(value);
        if (!operand &&
            (operators_[newIndex]->isSource() ||
             !operators_[newIndex]->isStreaming())) {
          operand = operators_[newIndex]->definesSubfield(
              *this, outputType->childAt(i), name, newIndex == 0);
        }
        if (operand) {
          operators_[newIndex]->addOutputId(operand->id);
          definedBy_[value] = operand;
          operandOperatorIndex_[operand] = operators_.size() - 1;
        }
      }
    }
    for (auto& [op, channel] : identityProjected) {
      Value value(toSubfield(outputType->nameOf(channel)));
      auto newOp = addIdentityProjections(op);
      projectedTo_[value] = newOp;
    }
    inputType = outputType;
  }
  if (operators_.empty()) {
    return false;
  }
  for (auto& op : operators_) {
    op->finalize(*this);
  }
  std::vector<OperandId> resultOrder;
  for (auto i = 0; i < outputType->size(); ++i) {
    auto operand = findCurrentValue(Value(toSubfield(outputType->nameOf(i))));
    resultOrder.push_back(operand->id);
  }
  auto waveOpUnique = std::make_unique<WaveDriver>(
      driver_.driverCtx(),
      outputType,
      operators[first]->planNodeId(),
      operators[first]->operatorId(),
      std::move(arena_),
      std::move(operators_),
      std::move(resultOrder),
      std::move(subfields_),
      std::move(operands_));
  auto waveOp = waveOpUnique.get();
  waveOp->initialize();
  std::vector<std::unique_ptr<exec::Operator>> added;
  added.push_back(std::move(waveOpUnique));
  auto replaced = driverFactory_.replaceOperators(
      driver_, first, operatorIndex, std::move(added));
  waveOp->setReplaced(std::move(replaced));
  return true;
}

bool waveDriverAdapter(
    const exec::DriverFactory& factory,
    exec::Driver& driver) {
  CompileState state(factory, driver);
  return state.compile();
}

void registerWave() {
  exec::DriverAdapter waveAdapter{"Wave", {}, waveDriverAdapter};
  exec::DriverFactory::registerAdapter(waveAdapter);
}
} // namespace facebook::velox::wave
