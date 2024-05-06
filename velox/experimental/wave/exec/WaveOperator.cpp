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

#include "velox/experimental/wave/exec/WaveOperator.h"
#include "velox/experimental/wave/exec/ToWave.h"
#include "velox/experimental/wave/exec/WaveDriver.h"

namespace facebook::velox::wave {

WaveOperator::WaveOperator(
    CompileState& state,
    const RowTypePtr& type,
    const std::string& planNodeId)
    : id_(state.numOperators()), planNodeId_(planNodeId), outputType_(type) {}

AbstractOperand* WaveOperator::definesSubfield(
    CompileState& state,
    const TypePtr& type,
    const std::string& parentPath,
    bool sourceNullable) {
  switch (type->kind()) {
    case TypeKind::ROW: {
      auto& row = type->as<TypeKind::ROW>();
      for (auto i = 0; i < type->size(); ++i) {
        auto& child = row.childAt(i);
        auto name = row.nameOf(i);
        std::string childPath = fmt::format("{}.{}", parentPath, name);
        definesSubfield(state, child, childPath, sourceNullable);
      }
    }
      [[fallthrough]];
      // TODO:Add cases for nested types.
    default: {
      auto field = state.toSubfield(parentPath);
      subfields_.push_back(field);
      types_.push_back(type);
      auto operand = state.findCurrentValue(Value(field));
      if (!operand) {
        operand = state.newOperand(type, parentPath);
      }
      if (sourceNullable && !operand->notNull && !operand->conditionalNonNull) {
        operand->sourceNullable = true;
      }
      defines_[Value(field)] = operand;

      return operand;
    }
  }
}

folly::Synchronized<exec::OperatorStats>& WaveOperator::stats() {
  return driver_->stats();
}

std::string WaveOperator::toString() const {
  std::stringstream out;
  out << "Id: " << id_ << " produces " << outputIds_.toString() << std::endl;
  return out.str();
}

} // namespace facebook::velox::wave
