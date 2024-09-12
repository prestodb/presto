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

#include "velox/exec/Operator.h"
#include "velox/experimental/wave/exec/AggregateFunctionRegistry.h"
#include "velox/experimental/wave/exec/WaveOperator.h"
#include "velox/expression/Expr.h"

namespace facebook::velox::wave {

using SubfieldMap =
    folly::F14FastMap<std::string, std::unique_ptr<common::Subfield>>;

class CompileState {
 public:
  CompileState(const exec::DriverFactory& driverFactory, exec::Driver& driver)
      : driverFactory_(driverFactory), driver_(driver) {}

  exec::Driver& driver() {
    return driver_;
  }

  // Replaces sequences of Operators in the Driver given at construction with
  // Wave equivalents. Returns true if the Driver was changed.
  bool compile();

  common::Subfield* toSubfield(const exec::Expr& expr);

  common::Subfield* toSubfield(const std::string& name);

  AbstractOperand* newOperand(AbstractOperand& other);

  AbstractOperand* newOperand(
      const TypePtr& type,
      const std::string& label = "");

  Program* newProgram();

  Value toValue(const exec::Expr& expr);

  Value toValue(const core::FieldAccessTypedExpr& field);

  AbstractOperand* addIdentityProjections(AbstractOperand* source);
  AbstractOperand* findCurrentValue(Value value);

  AbstractOperand* findCurrentValue(
      const std::shared_ptr<const core::FieldAccessTypedExpr>& field) {
    Value value = toValue(*field);
    return findCurrentValue(value);
  }

  AbstractOperand* addExpr(const exec::Expr& expr);

  void addInstruction(
      std::unique_ptr<AbstractInstruction> instruction,
      AbstractOperand* result,
      const std::vector<Program*>& inputs);

  std::vector<AbstractOperand*>
  addExprSet(const exec::ExprSet& set, int32_t begin, int32_t end);
  std::vector<std::vector<ProgramPtr>> makeLevels(int32_t startIndex);

  GpuArena& arena() const {
    return *arena_;
  }

  int numOperators() const {
    return operators_.size();
  }

  GpuArena& arena() {
    return *arena_;
  }

 private:
  bool
  addOperator(exec::Operator* op, int32_t& nodeIndex, RowTypePtr& outputType);

  void addFilter(const exec::Expr& expr, const RowTypePtr& outputType);

  AbstractState* newState(
      StateKind kind,
      const std::string& idString,
      const std::string& label);

  void addFilterProject(
      exec::Operator* op,
      RowTypePtr& outputType,
      int32_t& nodeIndex);

  /// Adds a projection operator containing programs starting at 'firstProgram'
  /// for the rest of 'allPrograms_'..
  void makeProject(int32_t firstProgram, RowTypePtr outputType);

  void makeAggregateLayout(AbstractAggregation& aggregate);

  void setAggregateFromPlan(
      const core::AggregationNode::Aggregate& planAggregate,
      AbstractAggInstruction& agg);

  void makeAggregateAccumulate(const core::AggregationNode* node);

  bool reserveMemory();

  // Adds 'instruction' to the suitable program and records the result
  // of the instruction to the right program. The set of programs
  // 'instruction's operands depend is in 'programs'. If 'instruction'
  // depends on all immutable programs, start a new one. If all
  // dependences are from the same open program, add the instruction
  // to that. If Only one of the programs is mutable, ad the
  // instruction to that.
  void addInstruction(
      std::unique_ptr<Instruction> instruction,
      const AbstractOperand* result,
      const std::vector<Program*>& inputs);

  void setConditionalNullable(AbstractBinary& binary);

  // Adds 'op->id' to 'nullableIf' if not already there.
  void addNullableIf(
      const AbstractOperand* op,
      std::vector<OperandId>& nullableIf);

  Program* programOf(AbstractOperand* op, bool create = true);

  const std::shared_ptr<aggregation::AggregateFunctionRegistry>&
  aggregateFunctionRegistry();

  std::unique_ptr<GpuArena> arena_;
  // The operator and output operand where the Value is first defined.
  DefinesMap definedBy_;

  // The Operand where Value is available after all projections placed to date.
  DefinesMap projectedTo_;

  // Index of WaveOperator producing the operand.
  folly::F14FastMap<AbstractOperand*, int32_t> operandOperatorIndex_;

  folly::F14FastMap<AbstractOperand*, Program*> definedIn_;

  const exec::DriverFactory& driverFactory_;
  exec::Driver& driver_;
  SubfieldMap subfields_;

  std::vector<ProgramPtr> allPrograms_;

  std::vector<std::vector<ProgramPtr>> pendingLevels_;

  // All AbstractOperands. Handed off to WaveDriver after plan conversion.
  std::vector<std::unique_ptr<AbstractOperand>> operands_;
  std::vector<std::unique_ptr<AbstractState>> operatorStates_;

  // The Wave operators generated so far.
  std::vector<std::unique_ptr<WaveOperator>> operators_;

  // The program being generated.
  std::shared_ptr<Program> currentProgram_;

  // Boolean to select the instruction. Set for conditionl sections.
  AbstractOperand* predicate_{nullptr};

  // Sequence number for operands.
  int32_t operandCounter_{0};
  int32_t wrapCounter_{0};
  int32_t stateCounter_{0};
  InstructionStatus instructionStatus_;

  // All InstructionStatus records in instructions that have them. Used for
  // patching the final grid size when this is known.
  std::vector<InstructionStatus*> allStatuses_;

  int32_t nthContinuable_{0};
  std::shared_ptr<aggregation::AggregateFunctionRegistry>
      aggregateFunctionRegistry_;
  folly::F14FastMap<std::string, std::shared_ptr<exec::Expr>> fieldToExpr_;
};

/// Registers adapter to add Wave operators to Drivers.
void registerWave();

} // namespace facebook::velox::wave
