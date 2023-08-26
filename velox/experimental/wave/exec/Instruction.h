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

struct AbstractOperand {
  AbstractOperand(int32_t id, const TypePtr& type, std::string label)
      : id(id), type(type), label(label) {}

  AbstractOperand(const AbstractOperand& other, int32_t id)
      : id(id), type(other.type), label(other.label) {}

  const int32_t id;

  // Operand type.
  TypePtr type;

  // Label for debugging, e.g. column name or Expr::toString output.
  std::string label;

  // Vector with constant value, else nullptr.
  VectorPtr constant;

  // True if bits in nulls or boolean values are as a bit field. Need widening
  // to byte on device.
  bool flagsAsBits{false};
};

struct AbstractInstruction {
  AbstractInstruction(OpCode opCode) : opCode(opCode) {}

  template <typename T>
  T& as() {
    return *reinterpret_cast<T*>(this);
  }

  OpCode opCode;
};

struct AbstractFilter : public AbstractInstruction {
  AbstractOperand* flags;
  AbstractOperand* indices;
};

struct AbstractWrap : public AbstractInstruction {
  AbstractOperand indices;
  std::vector<AbstractOperand*> source;
  std::vector<AbstractOperand*> target;

  void addWrap(AbstractOperand* sourceOp, AbstractOperand* targetOp = nullptr) {
    if (std::find(source.begin(), source.end(), sourceOp) != source.end()) {
      return;
    }
    source.push_back(sourceOp);
    target.push_back(targetOp ? targetOp : sourceOp);
  }
};

struct AbstractBinary : public AbstractInstruction {
  AbstractBinary(
      OpCode opCode,
      AbstractOperand* left,
      AbstractOperand* right,
      AbstractOperand* result)
      : AbstractInstruction(opCode), left(left), right(right), result(result) {}

  AbstractOperand* left;
  AbstractOperand* right;
  AbstractOperand* result;
  AbstractOperand* predicate{nullptr};
  bool invert{false};
};

} // namespace facebook::velox::wave
