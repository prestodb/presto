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
#include "velox/experimental/wave/exec/WaveOperator.h"
#include "velox/expression/Expr.h"

namespace facebook::velox::wave {

using SubfieldMap =
    folly::F14FastMap<std::string, std::unique_ptr<common::Subfield>>;

class CompileState {
 public:
  CompileState(const exec::DriverFactory& driverFactory, exec::Driver& driver)
      : driverFactory_(driverFactory), driver_(driver) {}

  // Replaces sequences of Operators in the Driver given at construction with
  // Wave equivalents. Returns true if the Driver was changed.
  bool compile();

  common::Subfield* toSubfield(const exec::Expr& expr);

  common::Subfield* toSubfield(const std::string& name);

  AbstractOperand* newOperand(AbstractOperand& other);

  AbstractOperand* newOperand(
      const TypePtr& type,
      const std::string& label = "");

  Value toValue(const exec::Expr& expr);

  AbstractOperand* addIdentityProjections(Value value, Program* definedIn);
  AbstractOperand* findCurrentValue(Value value);
  AbstractOperand* addExpr(const exec::Expr& expr);

  void addExprSet(const exec::ExprSet& set, int32_t begin, int32_t end);

 private:
  bool
  addOperator(exec::Operator* op, int32_t& nodeIndex, RowTypePtr& outputType);

  void addFilterProject(exec::Operator* op);
  bool reserveMemory();

  std::unique_ptr<GpuArena> arena_;
  // The operator and output operand where the Value is first defined.
  folly::F14FastMap<Value, AbstractOperand*, ValueHasher, ValueComparer>
      definedBy_;

  // The Operand where Value is available after all projections placed to date.
  folly::F14FastMap<Value, AbstractOperand*, ValueHasher, ValueComparer>
      projectedTo_;

  folly::F14FastMap<AbstractOperand*, std::shared_ptr<Program>> definedIn_;

  // The programs that cam be added to. Any programs from previous operators
  // after which there is no cardinality change or shuffle.
  folly::F14FastMap<Value, std::shared_ptr<Program>, ValueHasher, ValueComparer>
      openPrograms_;

  const exec::DriverFactory& driverFactory_;
  exec::Driver& driver_;
  SubfieldMap subfields_;

  // All AbstractOperands. Handed off to WaveDriver after plan conversion.
  std::vector<std::unique_ptr<AbstractOperand>> operands_;

  // The Wave operators generated so far.
  std::vector<std::unique_ptr<WaveOperator>> operators_;

  // The program being generated.
  std::shared_ptr<Program> currentProgram_;

  // Sequence number for operands.
  int32_t operandCounter_{0};
};

/// Registers adapter to add Wave operators to Drivers.
void registerWave();

} // namespace facebook::velox::wave
