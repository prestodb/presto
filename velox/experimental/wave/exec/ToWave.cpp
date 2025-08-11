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
#include "velox/experimental/wave/exec/Project.h"
#include "velox/experimental/wave/exec/TableScan.h"
#include "velox/experimental/wave/exec/Values.h"
#include "velox/experimental/wave/exec/WaveDriver.h"
#include "velox/expression/ConstantExpr.h"
#include "velox/expression/FieldReference.h"

DEFINE_int64(velox_wave_arena_unit_size, 1 << 30, "Per Driver GPU memory size");

namespace facebook::velox::wave {

using exec::Expr;

WaveRegistry& waveRegistry() {
  static auto registry = std::make_unique<WaveRegistry>();
  return *registry;
}
AggregateRegistry& aggregateRegistry() {
  static auto registry = std::make_unique<AggregateRegistry>();
  return *registry;
}

CompileState::CompileState(
    const exec::DriverFactory& driverFactory,
    exec::Driver& driver)
    : driverFactory_(driverFactory),
      driver_(driver),
      runtime_(std::make_shared<WaveRuntimeObjects>()),
      subfields_(runtime_->subfields),
      operands_(runtime_->operands),
      operatorStates_(runtime_->states) {
  setDevice(getDevice());
  pool_ = driver_.driverCtx()->task->pool();
}

common::Subfield* CompileState::toSubfield(const Expr& expr) {
  std::string name = expr.toString();
  return toSubfield(name);
}

common::Subfield* CompileState::toSubfield(const std::string& name) {
  VELOX_CHECK(!namesResolved_);
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
  if (dynamic_cast<const exec::FieldReference*>(&expr)) {
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

Value CompileState::toValue(const core::FieldAccessTypedExpr& field) {
  auto it = fieldToExpr_.find(field.name());
  exec::Expr* expr;
  if (it != fieldToExpr_.end()) {
    expr = it->second.get();
  } else {
    auto name = field.name();
    static std::vector<exec::ExprPtr> empty;
    exec::ExprPtr newExpr =
        std::make_shared<exec::FieldReference>(field.type(), empty, name);
    expr = newExpr.get();
    fieldToExpr_[name] = std::move(newExpr);
  }
  return toValue(*expr);
}

AbstractOperand* CompileState::newOperand(AbstractOperand& other) {
  auto newOp = std::make_unique<AbstractOperand>(other, operandCounter_++);
  newOp->definingSegment = segments_.size() - 1;
  operands_.push_back(std::move(newOp));
  return operands_.back().get();
}

AbstractOperand* CompileState::newOperand(
    const TypePtr& type,
    const std::string& label) {
  operands_.push_back(
      std::make_unique<AbstractOperand>(operandCounter_++, type, label));
  auto op = operands_.back().get();
  op->definingSegment = segments_.size() - 1;
  return op;
}

AbstractState* CompileState::newState(
    StateKind kind,
    const std::string& idString,
    const std::string& label) {
  operatorStates_.push_back(
      std::make_unique<AbstractState>(stateCounter_++, kind, idString, label));
  auto state = operatorStates_.back().get();
  return state;
}

bool maybeNotNull(const AbstractOperand* op) {
  if (!op) {
    return true;
  }
  if (op->constant) {
    return !op->constant->isNullAt(0);
  }
  return op->conditionalNonNull || op->notNull || op->sourceNullable;
}

void CompileState::addNullableIf(
    const AbstractOperand* op,
    std::vector<OperandId>& nullableIf) {
  if (op->constant || op->notNull) {
    return;
  }
  if (std::find(nullableIf.begin(), nullableIf.end(), op->id) ==
      nullableIf.end()) {
    if (op->sourceNullable) {
      nullableIf.push_back(op->id);
    } else if (op->conditionalNonNull) {
      for (auto& i : op->nullableIf) {
        if (std::find(nullableIf.begin(), nullableIf.end(), i) ==
            nullableIf.end()) {
          nullableIf.push_back(i);
        }
      }
    }
  }
}

void CompileState::setConditionalNullable(AbstractOperand* op) {
  if (op->inputs.empty()) {
    return;
  }
  for (auto* input : op->inputs) {
    if (!maybeNotNull(input)) {
      return;
    }
  }
  op->conditionalNonNull = true;
  for (auto* input : op->inputs) {
    addNullableIf(input, op->nullableIf);
  }
}

bool CompileState::reserveMemory() {
  if (arena_) {
    return true;
  }
  auto* allocator = getAllocator(getDevice());
  arena_ =
      std::make_shared<GpuArena>(FLAGS_velox_wave_arena_unit_size, allocator);
  return true;
}

bool CompileState::compile() {
  auto operators = driver_.operators();

  int32_t first = 0;
  int32_t operatorIndex = 0;
  RowTypePtr outputType;
  // Make sure operator states are initialized.  We will need to inspect some of
  // them during the transformation.
  driver_.initializeOperators();
  RowTypePtr inputType;
  std::vector<OperandId> resultOrder;
  outputType = makeOperators(operatorIndex, resultOrder);
  if (operators_.empty()) {
    return false;
  }

  for (auto& op : operators_) {
    op->finalize(*this);
  }
  if (!reserveMemory()) {
    VELOX_FAIL("Failed to reserve unified memory for Wave");
  }
  auto waveOpUnique = std::make_unique<WaveDriver>(
      driver_.driverCtx(),
      outputType,
      operators[first]->planNodeId(),
      operators[first]->operatorId(),
      std::move(arena_),
      std::move(operators_),
      std::move(resultOrder),
      runtime_);
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
  auto state = std::make_shared<CompileState>(factory, driver);
  return state->compile();
}

bool AggregateRegistry::registerGenerator(
    std::string aggregateName,
    std::unique_ptr<AggregateGenerator> generator) {
  generators_[aggregateName] = std::move(generator);
  return true;
}

const AggregateGenerator* AggregateRegistry::getGenerator(
    const AggregateUpdate& update) {
  auto it = generators_.find(update.name);
  if (it == generators_.end()) {
    VELOX_USER_FAIL("No aggregate {}", update.name);
  }
  return it->second.get();
}

void registerWave() {
  exec::DriverAdapter waveAdapter{"Wave", {}, waveDriverAdapter};
  exec::DriverFactory::registerAdapter(waveAdapter);
  registerWaveFunctions();
}

std::string planToString(const core::PlanNode* plan) {
  return plan->toString(true, true);
}

} // namespace facebook::velox::wave
