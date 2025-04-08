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

#include "velox/experimental/wave/exec/HashGen.h"

namespace facebook::velox::wave {

bool joinTypeHasNullableKeys(core::JoinType joinType) {
  return joinType == core::JoinType::kRight ||
      joinType == core::JoinType::kFull;
}

bool joinTypeHasNext(core::JoinType joinType) {
  return true;
}

std::string makeJoinRow(
    CompileState& state,
    const std::vector<AbstractOperand*>& keys,
    const std::vector<AbstractOperand*>& dependent,
    core::JoinType joinType,
    int32_t id) {
  bool nullableKeys = joinTypeHasNullableKeys(joinType);
  bool hasNext = joinTypeHasNext(joinType);
  std::stringstream out;
  out << "struct HashRow" << id << " {\n";
  int32_t numNullableKeys = nullableKeys ? keys.size() : 0;
  int32_t numNullable = numNullableKeys + dependent.size();
  for (auto n = 0; n < numNullable; n += 32) {
    out << fmt::format("  uint32_t nulls{};\n", n / 32);
  }
  makeKeyMembers(keys, "key", out);
  makeKeyMembers(dependent, "dep", out);
  if (hasNext) {
    out << "HashRow" << id
        << "* next;\n"
           "  HashRow"
        << id << "** __device__ nextPtr() { return &next; }\n";
  } else {
    out << "  HashRow" << id << "** __device__ nextPtr() { return nullptr;}\n";
  }
  out << "\n};\n\n";
  return out.str();
}

void makeInitJoinRow(
    CompileState& state,
    const OpVector& keys,
    const OpVector& dependent,
    int32_t id,
    bool nullableKeys) {
  auto& out = state.generated();
  out << "  [&](HashRow" << id << "* row) {\n";
  int32_t numNullFlags = dependent.size() + (nullableKeys ? keys.size() : 0);
  for (auto i = 0; i < keys.size(); ++i) {
    auto* op = keys[i];
    state.ensureOperand(op);
    if (nullableKeys) {
      out << fmt::format(
          "   if (!{}) {{ row->key{} = {};}}\n",
          state.isNull(op),
          i,
          state.operandValue(op));
    } else {
      out << "    row->key" << i << " = " << state.operandValue(op) << ";\n";
    }
  }

  for (auto i = 0; i < dependent.size(); ++i) {
    auto op = dependent[i];
    state.ensureOperand(op);
    out << fmt::format(
        "   if (!{}) {{ row->dep{} = {};}}\n",
        state.isNull(op),
        i,
        state.operandValue(op));
  }
  for (auto i = 0; i < numNullFlags; i += 32) {
    out << fmt::format("   row->nulls{} = 0;\n", i / 32);
  }
  OpVector allColumns;
  if (nullableKeys) {
    allColumns = keys;
  }
  allColumns.insert(allColumns.end(), dependent.begin(), dependent.end());
  out << fmt::format(
      "  row->nulls0 = {};\n", initRowNullFlags(state, 0, keys.size(), keys));
  out << "}\n";
}

void makeRowRowCompare(
    CompileState& state,
    const std::vector<AbstractOperand*>& keys,
    int32_t id) {
  auto& out = state.inlines();
  out << "  bool __device__ compare(HashRow" << id << "* left, HashRow" << id
      << "* right) {\n";
  for (auto i = 0; i < keys.size(); ++i) {
    out << "    if (left->key" << i << " != right->key" << i
        << ") { return false; }\n";
  }
  out << "  return true;\n}\n";
}

void makeBuildOps(CompileState& state, const JoinBuild& build) {
  state.addInclude("velox/experimental/wave/common/Hash.h");
  state.addInclude("velox/experimental/wave/common/BitUtil.cuh");
  state.addInclude("velox/experimental/wave/common/HashTable.cuh");
  auto& out = state.inlines();
  out << makeJoinRow(
      state, build.keys, build.dependent, build.joinType, build.id);
  auto id = build.id;
  out << "struct HashOps" << id << " {\n"
      << "  HashOps" << id << "() = default;\n";

  makeRowHash(state, build.keys, false, id);
  makeRowRowCompare(state, build.keys, id);

  out << "};\n\n";

  state.addEntryPoint("facebook::velox::wave::buildTableKernel");
  out << "void __global__ buildTableKernel(GpuHashTable* table, void* voidRows, int32_t numRows) {\n"
         "  auto rows = reinterpret_cast<HashRow"
      << id
      << "*>(voidRows);\n"
         "  HashOps"
      << id
      << " ops;\n"
         "  table->joinBuild<HashRow"
      << id << ", HashOps" << id
      << ">(rows, numRows, ops);\n"
         "}\n";
}

std::string JoinBuild::toString() const {
  std::stringstream out;
  out << "JoinBuild {";
  for (auto& key : keys) {
    out << key->toString() << " ";
  }
  out << " -> ";
  for (auto& dep : dependent) {
    out << dep->toString() << " ";
  }
  out << std::endl;
  return out.str();
}

void JoinBuild::visitReferences(
    std::function<void(AbstractOperand*)> visitor) const {
  for (auto& k : keys) {
    visitor(k);
  }
  for (auto& d : dependent) {
    visitor(d);
  }
}

void checkNullBuildKey(
    CompileState& state,
    const std::vector<AbstractOperand*>& keys,
    int32_t syncLabel) {
  auto& out = state.generated();
  for (auto* key : keys) {
    state.ensureOperand(key);
    out << fmt::format(
        "  if ({}) {{goto sync{};}}\n", state.isNull(key), syncLabel);
  }
}

void JoinBuild::generateMain(CompileState& state, int32_t syncLabel) {
  makeBuildOps(state, *this);
  auto& out = state.generated();
  state.declareNamed("GpuHashTable*", fmt::format("table{}", id), "0xdeadbeef");
  checkNullBuildKey(state, keys, syncLabel);
  out << "  table" << id << " = reinterpret_cast<GpuHashTable*>(shared->states["
      << state.stateOrdinal(*this->state)
      << "]);\n"
         "    if (!table"
      << id << "->addJoinRow<HashRow" << id << ">(";
  makeInitJoinRow(state, keys, dependent, id, false);
  out << ")) {\n"
         "     laneStatus = ErrorCode::kInsufficientMemory;\n"
         "      shared->hasContinue = true;\n"
         "    }\n";
  out << " sync" << syncLabel << ":\n";
  out << "      __syncthreads();\n";
  out << "  if (threadIdx.x == 0 && shared->hasContinue) {\n"
         "    auto ret = gridStatus<BuildReturn>(shared, "
      << abstractHashBuild->mutableInstructionStatus()->gridState
      << ");\n"
         "    ret->needMore = true;\n"
         "  }\n";
  out << "  __syncthreads();\n";
}

std::string JoinBuild::preContinueCode(CompileState& state) {
  return "    laneStatus = laneStatus == ErrorCode::kInsufficientMemory\n"
         "      ? ErrorCode::kOk : ErrorCode::kInactive;\n";
}

std::unique_ptr<AbstractInstruction> JoinBuild::addInstruction(
    CompileState& state) {
  auto result =
      std::make_unique<AbstractHashBuild>(state.nextSerial(), this->state);
  bool hasNext = joinTypeHasNext(joinType);
  bool nullableKeys = joinTypeHasNullableKeys(joinType);
  int32_t offset = 0;
  offset +=
      bits::roundUp((nullableKeys ? keys.size() : 0) + dependent.size(), 32) /
      8;
  for (auto& key : keys) {
    int32_t align = cudaTypeAlign(*key->type);
    int32_t width = cudaTypeSize(*key->type);
    offset = bits::roundUp(offset, align) + width;
  }
  for (auto& key : dependent) {
    int32_t align = cudaTypeAlign(*key->type);
    int32_t width = cudaTypeSize(*key->type);
    offset = bits::roundUp(offset, align) + width;
  }

  if (hasNext) {
    offset = bits::roundUp(offset, 8) + 8;
  }
  result->roundedRowSize = bits::roundUp(offset, 8);

  result->continueLabel = continueLabel_;
  result->joinBridge = joinBridge;
  abstractHashBuild = result.get();
  return result;
}

void JoinProbe::visitReferences(
    std::function<void(AbstractOperand*)> visitor) const {
  for (auto& key : keys) {
    visitor(key);
  }
}

void JoinProbe::visitResults(
    std::function<void(AbstractOperand*)> visitor) const {
  visitor(hits);
}

const char* probeBoilerPlate =
    "  table$I$ = reinterpret_cast<GpuHashTable*>(shared->states[$SI$]);\n"
    "  r$HIT$ = reinterpret_cast<int64_t>(table$I$->joinProbe<HashRow$I$>(hash$I$, ";

// List the non-key extracted build side columns  in the order of build side
// layout.
std::vector<AbstractOperand*> probeDependent(const JoinExpand* expand) {
  std::vector<AbstractOperand*> deps(
      expand->tableType->size() - expand->numKeys);
  for (auto i = 0; i < expand->tableChannels.size(); ++i) {
    auto channel = expand->tableChannels[i];
    if (channel < expand->numKeys) {
      continue;
    }
    deps[channel - expand->numKeys] = expand->dependent[i];
  }
  return deps;
}

void JoinProbe::generateMain(CompileState& state, int32_t syncLabel) {
  state.addInclude("velox/experimental/wave/common/Hash.h");
  state.addInclude("velox/experimental/wave/common/HashTable.cuh");

  state.inlines() << makeJoinRow(
      state, keys, probeDependent(expand), joinType, id);

  auto& out = state.generated();
  state.declareNamed(fmt::format("bool nullProbe{};", id));
  state.declareNamed(fmt::format("uint64_t hash{};", id));

  auto stateOrd = state.stateOrdinal(*this->state);
  state.declareNamed("GpuHashTable*", fmt::format("table{}", id), "0xdeadbeef");
  out << fmt::format("  nullProbe{} = false;\n", id);
  makeHash(state, keys, false, fmt::format("  nullProbe{} = true;", id), id);
  auto hitsOrdinal = state.declareVariable(*hits);
  auto temp = replaceAll(probeBoilerPlate, "$I$", fmt::format("{}", id));
  out << replaceAll(
      replaceAll(temp, "$SI$", fmt::format("{}", stateOrd)),
      "$HIT$",
      fmt::format("{}", hitsOrdinal));
  makeCompareLambda(state, keys, false, id);
  out << "));\n";
  auto flags = state.flags(*hits);
  flags.needStore = true;
  hits->isStored = true;
  hits->inRegister = true;
  if (flags.needStore) {
    out << fmt::format(
        "  flatOperand<int64_t>(operands, {}, blockBase) = r{};\n",
        hitsOrdinal,
        hitsOrdinal);
  }
  out << fmt::format("  continue{}: ;\n", expand->continueLabel_);
}

void JoinExpand::visitReferences(
    std::function<void(AbstractOperand*)> visitor) const {
  visitor(hits);
  if (filter) {
    visitor(filter);
  }
}

void JoinExpand::visitResults(
    std::function<void(AbstractOperand*)> visitor) const {
  visitor(indices);
  for (auto& r : dependent) {
    visitor(r);
  }
}

void makeCopyRow(CompileState& state, const JoinExpand& expand) {
  auto& out = state.generated();
  out << "[&](HashRow" << expand.id << "* row, int32_t nth) {\n";
  for (auto i = 0; i < expand.dependent.size(); ++i) {
    auto tableOrd = expand.tableChannels[i];
    std::string field;
    int32_t nullFlag;
    auto* op = expand.dependent[i];
    if (tableOrd < expand.numKeys) {
      field = fmt::format("key{}", tableOrd);
      nullFlag = expand.nullableKeys ? tableOrd : -1;
    } else {
      field = fmt::format("dep{}", tableOrd - expand.numKeys);
      nullFlag = expand.nullableKeys ? tableOrd : tableOrd - expand.numKeys;
    }
    if (nullFlag != -1) {
      out << fmt::format(
          "   setNull(operands, {}, blockBase, (row->nulls{} & {}) == 0);\n",
          state.ordinal(*op),
          nullFlag / 32,
          (1 << (nullFlag & 31)));
    }
    out << fmt::format(
        "  flatOperand<{}>(operands, {}, blockBase) = row->{};\n",
        cudaTypeName(*op->type),
        state.ordinal(*op),
        field);
  }
  out << "}";
}

void JoinExpand::generateMain(CompileState& state, int32_t syncLabel) {
  state.addInclude("velox/experimental/wave/exec/Join.cuh");
  auto& out = state.generated();
  if (filter) {
    state.generateIsTrue(*filter);
  }
  out << fmt::format(" sync{}: ;\n", syncLabel);
  auto duplicatesStr = fmt::format(" table{}->hasDuplicates", id);
  auto* status = this->state->instruction->mutableInstructionStatus();

  // the table must be loaded on all lanes, active or not, continue or
  // not. See the access to 'hasDuplicates'.
  out << "  table" << id << " = reinterpret_cast<GpuHashTable*>(shared->states["
      << state.stateOrdinal(*this->state) << "]);\n";

  out << fmt::format(
      "  if (joinResult<HashRow{}, {}, {}, {}, {}, {}>(",
      id,
      state.ordinal(*indices),
      status->gridState,
      status->gridStateSize,
      status->blockState,
      state.ordinal(*hits));
  out << state.operandValue(hits) << ", "
      << (filter ? state.operandValue(filter) : "true")
      << ", shared->localContinue || shared->startLabel == " << continueLabel_
      << ", laneStatus,  shared," << duplicatesStr << ")) {\n";
  out << "    goto continue" << continueLabel_ << ";}\n";
  out << "  __syncthreads();\n";
  out << "  laneStatus = threadIdx.x < shared->numRows ? ErrorCode::kOk : ErrorCode::kInactive;\n";
  state.generateWrap(wrapInfo_, nthWrap, indices);
  out << "  laneStatus = threadIdx.x < shared->numRows ? ErrorCode::kOk : ErrorCode::kInactive;\n"
      << "  if (laneStatus != ErrorCode::kOk) {goto skip" << syncLabel
      << "; }\n";

  auto tpl = fmt::format("HashRow{}, {}", id, state.ordinal(*hits));
  out << "  joinRow<" << tpl << ">(laneStatus, shared, ";
  makeCopyRow(state, *this);
  out << ");\n";
  out << "  skip" << syncLabel << ": ;\n";
}

std::string JoinExpand::preContinueCode(CompileState& state) {
  std::stringstream out;
  auto* status = this->state->instruction->mutableInstructionStatus();
  int32_t ord = state.ordinal(*hits);
  out << fmt::format(
      "  r{} = loadJoinNext<{}, {}>(shared, laneStatus);\n",
      ord,
      status->gridStateSize,
      status->blockState);
  if (state.flags(*hits).needStore) {
    out << fmt::format(
        "  flatOperand<int64_t>(operands, {}, blockBase) = r{};\n", ord, ord);
  }
  return out.str();
}

std::unique_ptr<AbstractInstruction> JoinExpand::addInstruction(
    CompileState& state) {
  auto result =
      std::make_unique<AbstractHashJoinExpand>(state.nextSerial(), this->state);
  result->joinBridge = joinBridge;
  result->planNodeId = planNodeId;
  result->continueLabel = continueLabel_;
  return result;
}

std::string JoinProbe::toString() const {
  std::stringstream out;
  out << "JoinProbe {";
  for (auto& key : keys) {
    out << key->toString() << " ";
  }
  out << "}\n";
  if (hits) {
    out << "  row=" << hits->toString() << "\n";
  }

  return out.str();
}

std::string JoinExpand::toString() const {
  std::stringstream out;
  out << "JoinExpand {";
  if (filter) {
    out << " filter = " << filter->toString() << std::endl;
  }
  out << " result = {{";
  for (auto& dep : dependent) {
    out << dep->toString() << " ";
  }
  out << "}\n";

  return out.str();
}
} // namespace facebook::velox::wave
