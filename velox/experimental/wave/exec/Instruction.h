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

#include "velox/experimental/wave/exec/ExprKernel.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"

namespace facebook::velox::wave {
/// Abstract representation of Wave instructions. These translate to a device
/// side ThreadBlockProgram right before execution.

template <typename T, typename U>
T addBytes(U* p, int32_t bytes) {
  return reinterpret_cast<T>(reinterpret_cast<uintptr_t>(p) + bytes);
}

/// Represents an input/output of an instruction or WaveOperator on host. The
/// device-side Operator is made at launch time based on this.
struct AbstractOperand {
  static constexpr int32_t kNoConstant = ~0;
  static constexpr int32_t kNoWrap = ~0;

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

  // Ordinal of the wrap instruction that first wraps this. All operands wrapped
  // by the same wrap share 'Operand.indices'. All Operands that are wrapped at
  // some point get indices when first created. When they get wrapped, there is
  // one wrap for all Operands with the same 'wrappedAt'
  int32_t wrappedAt{kNoWrap};

  std::string toString() const;
};

struct AbstractInstruction {
  AbstractInstruction(OpCode opCode) : opCode(opCode) {}

  virtual ~AbstractInstruction() = default;

  template <typename T>
  T& as() {
    return *reinterpret_cast<T*>(this);
  }

  OpCode opCode;

  virtual std::string toString() const {
    return fmt::format("OpCode {}", static_cast<int32_t>(opCode));
  }
};

struct AbstractReturn : public AbstractInstruction {
  AbstractReturn() : AbstractInstruction(OpCode::kReturn) {}
};

struct AbstractFilter : public AbstractInstruction {
  AbstractFilter(AbstractOperand* flags, AbstractOperand* indices)
      : AbstractInstruction(OpCode::kFilter), flags(flags), indices(indices) {}

  AbstractOperand* flags;
  AbstractOperand* indices;

  std::string toString() const override;
};

struct AbstractWrap : public AbstractInstruction {
  AbstractWrap(AbstractOperand* indices, int32_t id)
      : AbstractInstruction(OpCode::kWrap), indices(indices), id(id) {}
  AbstractOperand* indices;
  std::vector<AbstractOperand*> source;
  std::vector<AbstractOperand*> target;

  const int32_t id;
  // Offset of array of affected operand indices in the literals section of the
  // TB program. Filled in by first pass of making the TB program.
  int32_t literalOffset{-1};

  void addWrap(AbstractOperand* sourceOp, AbstractOperand* targetOp = nullptr) {
    int newWrap = AbstractOperand::kNoWrap;
    if (targetOp) {
      targetOp->wrappedAt = id;
    } else if (sourceOp->wrappedAt == AbstractOperand::kNoWrap) {
      sourceOp->wrappedAt = id;
    }

    for (auto i = 0; i < source.size(); ++i) {
      // If the operand has the same wrap as another one here, do nothing.
      if (source[i]->wrappedAt == sourceOp->wrappedAt ||
          (targetOp && target[i]->wrappedAt == targetOp->wrappedAt)) {
        return;
      }
    }
    source.push_back(sourceOp);
    target.push_back(targetOp ? targetOp : sourceOp);
  }

  std::string toString() const override;
};

struct AbstractBinary : public AbstractInstruction {
  AbstractBinary(
      OpCode opCode,
      AbstractOperand* left,
      AbstractOperand* right,
      AbstractOperand* result,
      AbstractOperand* predicate = nullptr)
      : AbstractInstruction(opCode),
        left(left),
        right(right),
        result(result),
        predicate(predicate) {}

  AbstractOperand* left;
  AbstractOperand* right;
  AbstractOperand* result;
  AbstractOperand* predicate;

  std::string toString() const override;
};

struct AbstractLiteral : public AbstractInstruction {
  AbstractLiteral(
      const VectorPtr& constant,
      AbstractOperand* result,
      AbstractOperand* predicate)
      : AbstractInstruction(OpCode::kLiteral),
        constant(constant),
        result(result),
        predicate(predicate) {}
  VectorPtr constant;
  AbstractOperand* result;
  AbstractOperand* predicate;
};

struct AbstractUnary : public AbstractInstruction {
  AbstractUnary(
      OpCode opcode,
      AbstractOperand* input,
      AbstractOperand* result,
      AbstractOperand* predicate = nullptr)
      : AbstractInstruction(opcode),
        input(input),
        result(result),
        predicate(predicate) {}
  AbstractOperand* input;
  AbstractOperand* result;
  AbstractOperand* predicate;
};

} // namespace facebook::velox::wave
