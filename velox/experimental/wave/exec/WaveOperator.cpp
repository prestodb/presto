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
    : id_(state.numOperators()), planNodeId_(planNodeId), outputType_(type) {
  definesSubfields(state, outputType_);
}

void WaveOperator::definesSubfields(
    CompileState& state,
    const TypePtr& type,
    const std::string& parentPath) {
  switch (type->kind()) {
    case TypeKind::ROW: {
      auto& row = type->as<TypeKind::ROW>();
      for (auto i = 0; i < type->size(); ++i) {
        auto& child = row.childAt(i);
        auto name = row.nameOf(i);
        auto field = state.toSubfield(name);
        subfields_.push_back(field);
        types_.push_back(child);
        auto operand = state.findCurrentValue(Value(field));
        if (!operand) {
          operand = state.newOperand(child, name);
        }
        outputIds_.add(operand->id);
        defines_[Value(field)] = operand;
      }
    }
      [[fallthrough]];
      // TODO:Add cases for nested types.
    default: {
      return;
    }
  }
}

folly::Synchronized<exec::OperatorStats>& WaveOperator::stats() {
  return driver_->stats();
}

} // namespace facebook::velox::wave
