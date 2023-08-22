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
      std::make_unique<AbstractOperand>(operandCounter_++, type, ""));
  auto op = operands_.back().get();
  return op;
}

AbstractOperand* CompileState::addIdentityProjections(
    Value value,
    Program* definedIn) {
  AbstractOperand* result = nullptr;
  for (auto i = 0; i < operators_.size(); ++i) {
    if (auto operand = operators_[i]->defines(value)) {
      result = operand;
      continue;
    }
    if (!result) {
      continue;
    }
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
  if (it == projectedTo_.end()) {
    auto originIt = definedBy_.find(value);
    if (originIt == definedBy_.end()) {
      return nullptr;
    }

    auto& program = definedIn_[originIt->second];
    VELOX_CHECK(program);
    return addIdentityProjections(value, program.get());
  }
  return it->second;
}

std::optional<OpCode> binaryOpCode(const Expr& expr) {
  auto& name = expr.name();
  if (name == "PLUS") {
    return OpCode::kPlus;
  }
  return std::nullopt;
}

AbstractOperand* CompileState::addExpr(const Expr& expr) {
  auto value = toValue(expr);
  auto current = findCurrentValue(value);
  if (current) {
    return current;
  }

  if (auto* field = dynamic_cast<const exec::FieldReference*>(&expr)) {
    std::string name = expr.name();
    return currentProgram_->findOperand(Value(&expr));
  } else if (auto* constant = dynamic_cast<const exec::ConstantExpr*>(&expr)) {
    VELOX_UNSUPPORTED("No constants");
  } else if (dynamic_cast<const exec::SpecialForm*>(&expr)) {
    VELOX_UNSUPPORTED("No special forms");
  }
  auto opCode = binaryOpCode(expr);
  if (!opCode.has_value()) {
    VELOX_UNSUPPORTED("Expr not supported: {}", expr.toString());
  }
  auto result = newOperand(expr.type(), "r");
  currentProgram_->instructions_.push_back(std::make_unique<AbstractBinary>(
      opCode.value(),
      addExpr(*expr.inputs()[0]),
      addExpr(*expr.inputs()[1]),
      result));
  return result;
}

void CompileState::addExprSet(
    const exec::ExprSet& exprSet,
    int32_t begin,
    int32_t end) {
  for (auto i = begin; i < end; ++i) {
    addExpr(*exprSet.exprs()[i]);
  }
}

void CompileState::addFilterProject(exec::Operator* op) {
  auto filterProject = reinterpret_cast<exec::FilterProject*>(op);
  auto data = filterProject->exprsAndProjection();
  VELOX_CHECK(!data.hasFilter);
  std::vector<ProgramPtr> programs;
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

bool CompileState::addOperator(
    exec::Operator* op,
    int32_t& nodeIndex,
    RowTypePtr& outputType) {
  auto& name = op->stats().rlock()->operatorType;
  if (name == "Values") {
    if (!reserveMemory()) {
      return false;
    }
    operators_.push_back(std::make_unique<Values>(
        *this,
        *reinterpret_cast<const core::ValuesNode*>(
            driverFactory_.planNodes[nodeIndex].get())));
    outputType = driverFactory_.planNodes[nodeIndex]->outputType();
    return true;
  } else if (name == "FilterProject") {
    if (!reserveMemory()) {
      return false;
    }
    addFilterProject(op);
  } else {
    return false;
  }
  return true;
}

bool CompileState::compile() {
  auto operators = driver_.operators();
  auto& nodes = driverFactory_.planNodes;

  int32_t first = 0;
  int32_t operatorIndex = 0;
  int32_t nodeIndex = 0;
  RowTypePtr outputType;
  for (; operatorIndex < operators.size(); ++operatorIndex) {
    if (!addOperator(operators[operatorIndex], nodeIndex, outputType)) {
      break;
    }
    ++nodeIndex;
  }
  if (operators_.empty()) {
    return false;
  }

  auto waveOpUnique = std::make_unique<WaveDriver>(
      driver_.driverCtx(),
      outputType,
      operators[first]->planNodeId(),
      operators[first]->operatorId(),
      std::move(arena_),
      std::move(operators_),
      std::move(subfields_),
      std::move(operands_));
  auto waveOp = waveOpUnique.get();
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
  exec::DriverAdapter waveAdapter{"Wave", waveDriverAdapter};
  exec::DriverFactory::registerAdapter(waveAdapter);
}
} // namespace facebook::velox::wave
