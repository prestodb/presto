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

#include "velox/exec/Driver.h"
#include "velox/exec/Operator.h"
#include "velox/experimental/wave/common/ResultStaging.h"
#include "velox/experimental/wave/exec/ExprKernel.h"
#include "velox/expression/Expr.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"

namespace facebook::velox::exec {
class HashJoinBridge;
}

namespace facebook::velox::wave {
/// Abstract representation of Wave instructions. These translate to a kernel
/// right before execution.

template <typename T, typename U>
T addBytes(U* p, int32_t bytes) {
  return reinterpret_cast<T>(reinterpret_cast<uintptr_t>(p) + bytes);
}

/// Represents an input/output of an instruction or WaveOperator on host. The
/// device-side Operator is made at launch time based on this.
struct AbstractOperand {
  static constexpr int32_t kNoConstant = ~0;
  static constexpr int32_t kNoWrap = ~0;
  static constexpr int32_t kNotAccessed = ~0;
  static constexpr int32_t kNoNullBit = ~0;

  AbstractOperand(int32_t id, const TypePtr& type, std::string label)
      : id(id), type(type), label(label) {}

  AbstractOperand(const AbstractOperand& other, int32_t id)
      : id(id), type(other.type), label(other.label) {}

  const int32_t id;

  // Operand type.
  TypePtr type;

  // Label for debugging, e.g. column name or Expr::toString output.
  std::string label;

  // The Operand of this is nullable if the Operand at some nullableIf_ is
  // nullable.
  std::vector<OperandId> nullableIf;

  // Vector with constant value, else nullptr.
  VectorPtr constant;

  // True if bits in nulls or boolean values are as a bit field. Need widening
  // to byte on device.
  bool flagsAsBits{false};

  // Offset of the literal from the block of literals after the instructions.
  // The base array in Operand will be set to 'constantOffset + end of last
  // instruction'.
  int32_t literalOffset{kNoConstant};
  // true if null literal.
  bool literalNull{false};

  // True if the data needs no null flags. Applies to some intermediates like
  // selected rows or flags or values of compile-time known non-nulls.
  bool notNull{false};

  // True if nullability depends on the run-time nullability of Operands this
  // depends on. These are in 'nullableIf'.
  bool conditionalNonNull{false};

  // if true, nullability is set in WaveStream at the time of launching. Given
  // by e.g. file metadata but not set at plan time.
  bool sourceNullable{false};

  /// True if represents a column. May be subject to lazy load.
  bool isColumn{false};

  bool isHostReturn{false};

  /// Corresponding Expr. Needs to be set if inlinable.
  const exec::Expr* expr{nullptr};

  // 1:1 to inputs of 'expr'. The same Expr will be different trees of
  // AbstractOperand if these are in different conditional
  // branches. Dedupping CSEs is only for non-conditionally executed.
  std::vector<AbstractOperand*> inputs;

  // True if value must be stored in memory, e.g. accessed in different kernel
  // or if operand of retriable.
  bool needsStore{false};

  // True if this may need retry, e.g. like string concat that allocates.
  bool retriable{false};

  // True of a column whose first access is conditional, e.g. behind a filter or
  // join.
  bool maybeLazy{false};

  // True of a column whose only use is as argument of a single pushdown
  // compatible agg.
  bool aggPushdown{false};

  // Number of references.
  int32_t numUses{0};

  // Cost to compute, excl. children. Determines if worth storing or
  // recomputing.
  int32_t cost{0};

  // Cost to compute, incl children.
  int32_t costWithChildren{0};

  // Segment ordinal where value is generated.
  int32_t definingSegment{0};

  // Segment ordinal where value is first accessed.
  int32_t firstUseSegment{kNotAccessed};
  // Segment ordinal where value is last accessed.
  int32_t lastUseSegment{0};

  // Ordinal of the wrap instruction that first wraps this. All operands wrapped
  // by the same wrap share 'Operand.indices'. All Operands that are wrapped at
  // some point get indices when first created. When they get wrapped, there is
  // one wrap for all Operands with the same 'wrappedAt'
  int32_t wrappedAt{kNoWrap};

  /// If true, during code gen, r<ordinal(id)> has the value.
  bool inRegister{false};

  /// During codegen, true if the value is in operands[ordinal(id)]. Applies to
  /// expression results. Leaf columns are always stored.
  bool isStored{false};

  /// Bit field in register with null flags.
  int32_t registerNullBit{kNoNullBit};

  /// If the value is in a hash table row, this is the operand with the row.
  AbstractOperand* containerRow{nullptr};

  /// If 'containerRow' is set, this is the field on the row.
  std::string containerField;

  /// If true, has an element per thread block, not per thread.
  bool elementPerTB{false};

  std::string toString() const;
};

using OpVector = std::vector<AbstractOperand*>;
using OpCVector = std::vector<const AbstractOperand*>;

class WaveStream;
struct OperatorState;
struct LaunchControl;
struct AbstractInstruction;

struct AdvanceResult {
  bool empty() const {
    return numRows == 0 && !isRetry;
  }

  std::string toString() const;

  ///  Max number of result rows.
  int32_t numRows{0};

  /// The sequence number of kernel launch that needs continue. (level idx in
  /// Project).
  int32_t nthLaunch{0};

  /// The ordinal of the program in the launch.
  int32_t programIdx{0};

  /// The label where to pick up.
  int32_t continueLabel{0};

  // The instruction index in host side Program where to pick up. If not 0, must
  // have 'isRetry' true.
  int32_t instructionIdx{0};

  /// True if continuing execution of a partially executed instruction. false if
  /// getting a new batch from a source. If true, the kernel launch must specify
  /// continue in the next kernel launch.
  bool isRetry{false};

  /// Stop all Drivers in Task pipeline for the time of 'statusUpdate'. Use this
  /// for e.g. rehashing a table shared between all WaveDrivers.
  bool syncDrivers{false};

  /// Stop all streams in WaveDriver for the time of updateStatus(). Use
  bool syncStreams{false};

  /// Action to run before continue. If the update is visible between
  /// streams/Drivers, use the right sync flag above. No sync needed if e.g.
  /// adding space to a string buffer on the 'stream's' vectors.
  std::function<void(
      WaveStream&,
      const std::vector<WaveStream*>& otherStreams,
      AbstractInstruction&)>
      updateStatus{nullptr};

  /// Extra token to mark reason for 'syncDrivers', e.g. the host side
  /// handle to a device hash table to rehash.
  void* reason{nullptr};
};

/// Opcodes for abstract instructions that have a host side representation and
/// status.
enum class OpCode { kAggregate, kReadAggregate, kHashBuild, kHashJoinExpand };

class Program;

struct AbstractInstruction {
  AbstractInstruction(OpCode opCode, int32_t serial = -1)
      : opCode(opCode), serial(serial) {}

  virtual ~AbstractInstruction() = default;

  template <typename T>
  T& as() {
    return *reinterpret_cast<T*>(this);
  }

  /// Checks blocking for external reasons for a source instruction in a source
  /// executable. Applies to e.g. exchange.
  virtual exec::BlockingReason isBlocked(
      WaveStream& stream,
      OperatorState* state,
      ContinueFuture* future) const {
    return exec::BlockingReason::kNotBlocked;
  }

  /// Prepares the source instruction of a Program that begins with a
  /// source instruction, like reading an aggregation or an
  /// exchange. 'state' is a handle to the state on device. The
  /// Executable is found in 'stream'. The 'this' contains no state
  /// and is only used to dispatch on the operator.
  virtual AdvanceResult canAdvance(
      WaveStream& stream,
      LaunchControl* control,
      OperatorState* state,
      int32_t instructionIdx) const {
    return {};
  }

  /// Called on each instruction of a pipeline after the pipeline is
  /// at end on all Drivers. This will take place on the last stream
  /// of the last Driver to finish. This can produce a combined result
  /// like a hash join build side.
  virtual void pipelineFinished(WaveStream& /*stream*/, Program* /*program*/) {}

  virtual bool isSink() const {
    return false;
  }

  virtual std::optional<int32_t> stateId() const {
    return std::nullopt;
  }

  /// Returns a function that sets up a state, e.g. hash table, given a
  /// WaveStream.
  virtual std::function<std::shared_ptr<OperatorState>(WaveStream& stream)>
  stateCreateFunction() {
    return nullptr;
  }

  /// True if assigns 'op'.
  virtual bool isOutput(const AbstractOperand* op) const {
    return false;
  }

  virtual bool isContinuable(WaveStream& stream) const {
    return false;
  }

  /// Returns the instructionIdx to use in AdvanceResult to pick up from 'this'.
  virtual int32_t continueIdx() const {
    return serial;
  }

  virtual void reserveState(InstructionStatus& state) {}

  /// Returns the InstructionStatus if any. Used for patching the grid
  /// size after all statuses in the operator pipeline are known.
  virtual InstructionStatus* mutableInstructionStatus() {
    return nullptr;
  }

  OpCode opCode;

  virtual std::string toString() const {
    return fmt::format("OpCode {}", static_cast<int32_t>(opCode));
  }

  int32_t serial{-1};
};

enum class StateKind : uint8_t { kGroupBy, kHashBuild };

/// Represents a shared state operated on by instructions. For example, a
/// join/group by table, destination buffers for repartition etc. Device side
/// memory owned by a group of WaveBufferPtrs in the Program or WaveStream.
struct AbstractState {
  AbstractState(
      int32_t id,
      StateKind kind,
      const std::string& idString,
      const std::string& label)
      : id(id), kind(kind), idString(idString), label(label) {}

  /// serial numbr.
  int32_t id;

  StateKind kind;

  /// PlanNodeId for joins/aggregates, other velox::Task scoped id.
  std::string idString;

  /// Comment
  std::string label;

  /// True if there is one item per WaveDriver, If false, there is one item per
  /// WaveStream.
  bool isGlobal;
  AbstractInstruction* instruction;
};

struct AbstractOperator : public AbstractInstruction {
  AbstractOperator(
      OpCode opCode,
      int32_t serial,
      AbstractState* state,
      RowTypePtr outputType = nullptr)
      : AbstractInstruction(opCode, serial),
        state(state),
        outputType(outputType) {}

  std::optional<int32_t> stateId() const override {
    if (!state) {
      return std::nullopt;
    }
    return state->id;
  }

  // Handle on device side state, e.g. aggregate hash table or repartitioning
  // output buffers.
  AbstractState* state;
  RowTypePtr outputType;
};

/// Describes a field in a row-wise container for hash build/group by.
struct AbstractField {
  TypePtr type;
  int32_t fieldIdx;
  int32_t nullIdx{-1};
};

struct AbstractAggInstruction {
  std::vector<AbstractOperand*> args;
  AbstractOperand* result;
};

struct AbstractAggregation : public AbstractOperator {
  AbstractAggregation(
      int32_t serial,
      std::vector<AbstractOperand*> keys,
      std::vector<AbstractAggInstruction> aggregates,
      AbstractState* state,
      RowTypePtr outputType)
      : AbstractOperator(OpCode::kAggregate, serial, state, outputType),
        keys(std::move(keys)),
        aggregates(std::move(aggregates)) {}

  int32_t rowSize() {
    return roundedRowSize;
  }

  bool isSink() const override {
    return true;
  }

  void reserveState(InstructionStatus& state) override;

  InstructionStatus* mutableInstructionStatus() override {
    return &instructionStatus;
  }

  AdvanceResult canAdvance(
      WaveStream& stream,
      LaunchControl* control,
      OperatorState* state,
      int32_t instructionIdx) const override;

  std::function<std::shared_ptr<OperatorState>(WaveStream& stream)>
  stateCreateFunction() override;

  InstructionStatus instructionStatus;

  bool intermediateInput{false};
  bool intermediateOutput{false};
  std::vector<AbstractOperand*> keys;
  std::vector<AbstractField> keyFields;
  std::vector<AbstractAggInstruction> aggregates;

  /// Prepare up to this many result reading streams.
  int16_t maxReadStreams{1};

  int32_t roundedRowSize{0};
  int32_t continueLabel{-1};
};

struct AbstractReadAggregation : public AbstractOperator {
  AbstractReadAggregation(
      int32_t serial,
      AbstractAggregation* aggregation,
      int32_t continueLabel)
      : AbstractOperator(
            OpCode::kReadAggregate,
            serial,
            aggregation->state,
            aggregation->outputType),
        aggregation(aggregation),
        continueLabel(continueLabel) {}

  AdvanceResult canAdvance(
      WaveStream& stream,
      LaunchControl* control,
      OperatorState* state,
      int32_t instructionIdx) const override;

  AbstractAggregation* aggregation;
  int32_t literalOffset{0};
  int32_t continueLabel{-1};
};

struct AbstractHashBuild : public AbstractOperator {
  AbstractHashBuild(int32_t serial, AbstractState* state)
      : AbstractOperator(OpCode::kHashBuild, serial, state) {}

  bool isSink() const override {
    return true;
  }

  AdvanceResult canAdvance(
      WaveStream& stream,
      LaunchControl* control,
      OperatorState* state,
      int32_t instructionIdx) const override;

  void pipelineFinished(WaveStream& stream, Program* program) override;

  std::function<std::shared_ptr<OperatorState>(WaveStream& stream)>
  stateCreateFunction() override;

  void reserveState(InstructionStatus& state) override;

  InstructionStatus* mutableInstructionStatus() override {
    return &status;
  }

  int32_t rowSize() {
    return roundedRowSize;
  }

  int32_t roundedRowSize{-1};

  InstructionStatus status;
  int32_t continueLabel;
  std::shared_ptr<exec::HashJoinBridge> joinBridge;
};

struct AbstractHashJoinExpand : public AbstractOperator {
  AbstractHashJoinExpand(int32_t serial, AbstractState* state)
      : AbstractOperator(OpCode::kHashJoinExpand, 0, state) {}

  AdvanceResult canAdvance(
      WaveStream& stream,
      LaunchControl* control,
      OperatorState* state,
      int32_t instructionIdx) const override;

  exec::BlockingReason isBlocked(
      WaveStream& stream,
      OperatorState* state,
      ContinueFuture* future) const override;

  void reserveState(InstructionStatus& state) override;

  InstructionStatus* mutableInstructionStatus() override {
    return &status;
  }

  std::string planNodeId;
  std::shared_ptr<exec::HashJoinBridge> joinBridge;

  InstructionStatus status;
  int32_t continueLabel;
};

/// Serializes 'row' to characters interpretable on device.
std::string rowTypeString(const RowTypePtr& row);

} // namespace facebook::velox::wave
