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
#include "velox/functions/prestosql/aggregates/AggregateNames.h"

namespace facebook::velox::wave {

class SimpleAggregate : public AggregateGenerator {
 public:
  explicit SimpleAggregate(const std::string& binaryFunc)
      : AggregateGenerator(false), binaryFunc_(binaryFunc) {}

  void generateInline(
      CompileState& state,
      const AggregateProbe& probe,
      const AggregateUpdate& update) const override {
    std::vector<TypePtr> types;
    types.push_back(update.result->type);
    types.push_back(update.result->type);
    state.functionReferenced(binaryFunc_, types, types[0]);
  }

  void generateInclude(
      CompileState& state,
      const AggregateProbe& /*probe*/,
      const AggregateUpdate& /*update*/) const override {
    state.addInclude("velox/experimental/wave/exec/Accumulators.cuh");
  }

  std::pair<int32_t, int32_t> accumulatorSizeAndAlign(
      const AggregateUpdate& update) const override {
    return std::make_pair<int32_t, int32_t>(
        cudaTypeSize(*update.result->type),
        cudaTypeAlign(*update.result->type));
  }

  std::string generateAccumulator(
      CompileState& state,
      const AggregateProbe& probe,
      const AggregateUpdate& update) const override {
    std::stringstream out;
    out << cudaTypeName(*update.result->type) << " ";
    return out.str();
  }

  std::string generateInit(CompileState& state, const AggregateUpdate& update)
      const override {
    return fmt::format("  row->acc{} = 0;\n", update.accumulatorIdx);
  }

  bool hasAtomic() const override {
    return true;
  }

  virtual void makeDeduppedUpdate(
      CompileState& state,
      const AggregateProbe& probe,
      const AggregateUpdate& update) const override {
    if (probe.keys.empty()) {
      VELOX_NYI();
    } else {
      auto nullIdx = update.accumulatorIdx + probe.keys.size();
      auto reduceName = "plus";
      // fmt::format("plus<{}>", cudaTypeName(*update.args[0]->type));
      state.generated() << fmt::format(
          "  simpleAccumulate(peers, leader, laneId, &row->acc{}, keyNulls, &row->nulls{}, {}, {}, {}, {});\n",
          update.accumulatorIdx,
          nullIdx / 32,
          1U << nullIdx,
          state.operandValue(update.args[0]),
          state.isNull(update.args[0]),
          reduceName);
    }
  }

  void makeNonGroupedUpdate(
      CompileState& state,
      const AggregateProbe& probe,
      const AggregateUpdate& update) const override {
    auto& out = state.generated();

    out << fmt::format(
        "sumReduce<{}>({}, {}, laneStatus, accNulls, &row->acc{}, &row->nulls{}, {}, &shared->data);\n",
        cudaTypeName(*update.args[0]->type),
        state.operandValue(update.args[0]),
        state.isNull(update.args[0]),
        update.accumulatorIdx,
        update.accumulatorIdx / 32,
        1U << (update.accumulatorIdx & 31));
  }

  std::string generateUpdate(
      CompileState& state,
      const AggregateProbe& probe,
      const AggregateUpdate& update) const override {
    VELOX_UNSUPPORTED();
    std::stringstream out;
    auto nullable = !update.args[0]->notNull;
    if (nullable) {
      out << fmt::format("   if (!{}) {{\n", state.isNull(update.args[0]));
    }
    if (binaryFunc_ == "plus") {
      out << fmt::format(
          "      atomicAdd(reinterpret_cast<{}*>(&row->acc{}), {});\n",
          cudaAtomicTypeName(*update.args[0]->type),
          update.accumulatorIdx,
          state.operandValue(update.args[0]));
    } else {
      VELOX_NYI("Only plus is supported as aggregate reduction");
    }
    if (nullable) {
      out << "}}\n";
    }
    return out.str();
  }

  std::string generateExtract(
      CompileState& state,
      const AggregateProbe& probe,
      const AggregateUpdate& update) const override {
    auto ord = state.ordinal(*update.result);
    auto nthNull = update.accumulatorIdx + probe.keys.size();
    return fmt::format(
        "   setNull(operands, {}, blockBase, (readRow->nulls{} & (1U << {})) == 0);\n"
        "    flatResult<{}>(operands, {}, blockBase) = readRow->acc{};\n",
        ord,
        nthNull / 32,
        nthNull & 31,
        cudaTypeName(*update.result->type),
        ord,
        update.accumulatorIdx);
  }

 protected:
  std::string binaryFunc_;
};

namespace {
bool temp = aggregateRegistry().registerGenerator(
    aggregate::kSum,
    std::make_unique<SimpleAggregate>("plus"));
}

} // namespace facebook::velox::wave
