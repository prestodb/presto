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

#include "velox/experimental/wave/exec/AggregateGen.h"

namespace facebook::velox::wave {

std::string makeAggregateRow(CompileState& state, const AggregateProbe& probe) {
  std::stringstream out;
  out << "struct HashRow {\n"
         "  int32_t flags;\n"
      << std::endl;
  int32_t numNullable = probe.keys.size() + probe.updates.size();
  for (auto n = 0; n < numNullable; n += 32) {
    out << fmt::format("  uint32_t nulls{};\n", n / 32);
  }
  makeKeyMembers(probe.keys, out);
  for (auto i = 0; i < probe.updates.size(); ++i) {
    probe.updates[i]->generator->generateInclude(
        state, probe, *probe.updates[i]);
    probe.updates[i]->generator->generateInline(
        state, probe, *probe.updates[i]);

    out << probe.updates[i]->generator->generateAccumulator(
               state, probe, *probe.updates[i])
        << std::endl
        << " acc" << i << ";\n";
  }
  out << "};\n\n";
  return out.str();
}

const char* aggregateOpsBoilerPlate =

    "HashRow* __device__\n"
    "  newRow(GpuHashTable* table, int32_t partition, int32_t i) {\n"
    "    auto* allocator = &table->allocators[partition];\n"
    "    return allocator->allocateRow<HashRow>();\n"
    "}\n"

    "  template <typename InitRow>\n"
    "  ProbeState __device__ insert(\n"
    "      GpuHashTable* table,\n"
    "      int32_t partition,\n"
    "      GpuBucket* bucket,\n"
    "      uint32_t misses,\n"
    "      uint32_t oldTags,\n"
    "      uint32_t tagWord,\n"
    "      int32_t i,\n"
    "      HashRow*& row,\n"
    "      InitRow init) {\n"
    "    if (!row) {\n"
    "      row = newRow(table, partition, i);\n"
    "      if (!row) {\n"
    "        return ProbeState::kNeedSpace;\n"
    "      }\n"
    "   init(row);\n"
    "    }\n"
    "    auto missShift = __ffs(misses) - 1;\n"
    "    if (!bucket->addNewTag(tagWord, oldTags, missShift)) {\n"
    "      return ProbeState::kRetry;\n"
    "    }\n"
    "    bucket->store(missShift / 8, row);\n"
    "    atomicInc(&table->numDistinct, static_cast<int64_t>(1));\n"
    "    return ProbeState::kDone;\n"
    "  }\n"
    "\n"
    "  void __device__ addHostRetry(int32_t i) {\n"
    "    shared->hasContinue = true;\n"
    "    shared->status[i / kBlockSize].errors[i & (kBlockSize - 1)] =\n"
    "        ErrorCode::kInsufficientMemory;\n"
    "  }\n"
    "\n"
    "  void __device__\n"
    "  freeInsertable(GpuHashTable* table, HashRow* row, uint64_t h) {\n"
    "    int32_t partition = table->partitionIdx(h);\n"
    "    auto* allocator = &table->allocators[partition];\n"
    "    allocator->markRowFree(row);\n"
    "  }\n"
    "\n"
    "  HashRow* __device__ getExclusive(\n"
    "      GpuHashTable* table,\n"
    "      GpuBucket* bucket,\n"
    "      HashRow* row,\n"
    "      int32_t hitIdx) {\n"
    "    return row;\n"
    "  }\n"
    "\n"
    "  void __device__ writeDone(HashRow* row) {}\n"
    "\n";

void makeAggregateOps(
    CompileState& state,
    const AggregateProbe& probe,
    bool forRead) {
  state.addInclude("velox/experimental/wave/common/Hash.h");
  state.addInclude("velox/experimental/wave/common/BitUtil.cuh");
  state.addInclude("velox/experimental/wave/common/HashTable.cuh");
  auto& out = state.inlines();
  out << makeAggregateRow(state, probe);

  out << "struct AggregateOps {\n"
      << "  AggregateOps() = default;\n"
      << "  __device__ AggregateOps(uint64_t hash, WaveShared* shared) : hashNumber(hash), shared(shared){}\n"
      << "  uint64_t hashNumber;\n"
      << "  WaveShared* shared;\n";
  if (forRead) {
  } else {
    out << "  uint64_t __device__ hash(int32_t /*i*/) const { return hashNumber; }\n";
    makeRowHash(state, probe.keys, true);
    out << aggregateOpsBoilerPlate;
  }
  out << "};\n\n";

  if (forRead) {
    return;
  }
  state.addEntryPoint("facebook::velox::wave::setupAggregationKernel");
  out << "void __global__ setupAggregationKernel(AggregationControl op) {\n"
         "  if (op.oldBuckets) {\n"
         "    auto table = op.head->table;\n"
         "    reinterpret_cast<GpuHashTable*>(table)->rehash<HashRow>(\n"
         "        reinterpret_cast<GpuBucket*>(op.oldBuckets),\n"
         "        op.numOldBuckets,\n"
         "        AggregateOps(0, nullptr));\n"
         "    return;\n"
         "  }\n"
         "  auto* data = new (op.head) DeviceAggregation();\n"
         "  data->rowSize = op.rowSize;\n"
         "  data->singleRow = reinterpret_cast<char*>(data + 1);\n"
         "  memset(data->singleRow, 0, op.rowSize);\n"
         "}\n";
}

/// Emits a lambda that performs the inlined aggregate update.
void makeUpdateLambda(
    CompileState& state,
    const AggregateProbe& probe,
    std::vector<const KernelStep*> updates) {
  auto& out = state.generated();

  out << "  [&](GpuHashTable* table, HashRow* row, uint32_t peers, int32_t leader, int32_t laneId) {\n";
  std::vector<const AggregateUpdate*> deferred;

  auto emitUpdates = [&](bool flush) {
    if (flush || deferred.size() > 4) {
      for (auto& update : deferred) {
        update->generator->makeDeduppedUpdate(state, probe, *update);
      }
      deferred.clear();
    }
  };
  for (auto lastIdx = 0; lastIdx < updates.size(); ++lastIdx) {
    auto* step = updates[lastIdx];
    if (step->kind() != StepKind::kAggregateUpdate) {
      const_cast<KernelStep*>(step)->generateMain(state, -1);
      continue;
    }
    auto& update = step->as<AggregateUpdate>();
    update.generator->loadArgs(state, probe, update);
    deferred.push_back(&update);
    emitUpdates(false);
  }
  emitUpdates(true);

  out << "  }";
}

std::string checkReturnBlockStatus() {
#ifdef BLOCK_STATUS_CHECK

  return "  if ((int)laneStatus > 4) {\n"
         "printf(\"bad laneStatus\\n\");\n"
         "  }\n";
#else
  return "";
#endif
}

void makeNonGroupedAggregation(
    CompileState& state,
    const AggregateProbe& probe,
    int32_t syncLabel) {
  auto& out = state.generated();
  out << fmt::format("  sync{}:\n", syncLabel);
  state.declareNamed("DeviceAggregation* state;");
  state.declareNamed("uint32_t accNulls;");
  out << fmt::format(
      "  state =\n"
      "    reinterpret_cast<DeviceAggregation*>(shared->states[{}]);\n",
      state.stateOrdinal(*probe.state));
  state.declareNamed("HashRow* row;");
  out << "  row = reinterpret_cast<HashRow*>(state->singleRow);\n";
  for (auto i = 0; i < probe.updates.size(); i += 32) {
    out << "  if (threadIdx.x == 0) {\n"
        << fmt::format("  accNulls = row->nulls{};\n", i / 32) << "  }\n";
    std::vector<const AggregateUpdate*> deferred;
    int32_t currentAccNulls = -1;
    auto emitUpdates = [&](bool flush) {
      if (flush || deferred.size() > 4) {
        for (auto& update : deferred) {
          if (update->accumulatorIdx / 32 != currentAccNulls) {
            out << fmt::format(
                "  accNulls = row->nulls{};\n", update->accumulatorIdx / 32);
            currentAccNulls = update->accumulatorIdx / 32;
          }
          update->generator->makeNonGroupedUpdate(state, probe, *update);
        }
        deferred.clear();
      }
    };
    for (auto lastIdx = i;
         lastIdx < probe.inlinedUpdates.size() && lastIdx < i + 32;
         ++lastIdx) {
      auto* step = probe.inlinedUpdates[lastIdx];
      if (step->kind() != StepKind::kAggregateUpdate) {
        const_cast<KernelStep*>(step)->generateMain(state, -1);
        continue;
      }
      auto& update = step->as<AggregateUpdate>();
      update.generator->loadArgs(state, probe, update);
      deferred.push_back(&update);
      emitUpdates(false);
    }
    emitUpdates(true);
  }
}

void makeAggregateProbe(
    CompileState& state,
    const AggregateProbe& probe,
    int32_t syncLabel) {
  if (probe.keys.empty()) {
    makeNonGroupedAggregation(state, probe, syncLabel);
    return;
  }
  auto& out = state.generated();
  state.declareNamed("uint64_t hash;");
  makeHash(state, probe.keys, true, "");
  state.declareNamed("AggregateOps ops;");
  out << "  ops = AggregateOps(hash, shared);\n";
  state.declareNamed("DeviceAggregation* state;");
  state.declareNamed("uint32_t keyNulls;");
  out << fmt::format(
      "  state =\n"
      "    reinterpret_cast<DeviceAggregation*>(shared->states[{}]);\n",
      state.stateOrdinal(*probe.state));
  state.declareNamed("GpuHashTable* table;");
  out << "  table = reinterpret_cast<GpuHashTable*>(state->table);\n";
  out << fmt::format(" sync{}:\n", syncLabel);
  out << "  shared->status->errors[threadIdx.x] = laneStatus;\n";
  out << "  table->updatingProbe<HashRow>(threadIdx.x, LaneId(), laneStatus == ErrorCode::kOk, ops, \n";
  makeCompareLambda(state, probe.keys, true);
  out << ",\n";
  makeInitGroupRow(state, probe.keys, probe.updates);
  out << ",\n";
  makeUpdateLambda(state, probe, probe.inlinedUpdates);
  out << ");\n";
  out << "      __syncthreads();\n"
         "  laneStatus = shared->status->errors[threadIdx.x];\n";
  out << checkReturnBlockStatus();
  out << "  if (threadIdx.x == 0 && shared->hasContinue) {\n"
         "    auto ret = gridStatus<AggregateReturn>(shared, "
      << probe.abstractAggregation->mutableInstructionStatus()->gridState
      << ");\n"
      // Must load 'state' and 'table' here because thread 0 might have
      // been inactive on entry and have 'table' uninited.
      << fmt::format(
             "  state =\n"
             "    reinterpret_cast<DeviceAggregation*>(shared->states[{}]);\n",
             state.stateOrdinal(*probe.state))
      << "  table = reinterpret_cast<GpuHashTable*>(state->table);\n"
         "    ret->numDistinct = table->numDistinct;\n"
         "  }\n"
         "  __syncthreads();\n"
         "  if (threadIdx.x == 0 && shared->isContinue) {\n"
         "    shared->isContinue = false;\n"
         "  }\n"
         "  __syncthreads();\n";
}

std::string readAggRow(CompileState& state, const ReadAggregation& read) {
  std::stringstream out;
  for (auto i = 0; i < read.funcs.size(); ++i) {
    auto& func = *read.funcs[i];
    out << func.generator->generateExtract(state, *read.probe, func);
  }
  return out.str();
}

void makeReadAggregation(CompileState& state, const ReadAggregation& read) {
  auto& out = state.generated();
  auto stateOrdinal = state.stateOrdinal(*read.state);
  state.declareNamed("DeviceAggregation* state;");
  if (read.probe->keys.empty()) {
    // Case with no grouping.
    out << "  if (threadIdx.x != 0) { laneStatus = ErrorCode::kInactive; } else {\n"
        << fmt::format(
               "  state =\n"
               "    reinterpret_cast<DeviceAggregation*>(shared->states[{}]);\n",
               stateOrdinal);
    out << "  HashRow* row = reinterpret_cast<HashRow*>(state->singleRow);\n";
    out << readAggRow(state, read);
    out << "    shared->status->numRows = 1;\n"
        << "  }\n";
    return;
  }
  out << fmt::format(
      "  state =\n"
      "    reinterpret_cast<DeviceAggregation*>(shared->states[{}]);\n",
      stateOrdinal);

  state.declareNamed("int32_t rowIdx;");
  state.declareNamed("int32_t numRows;");
  state.declareNamed("HashRow* row;");
  out << "  rowIdx = blockIdx.x * kBlockSize + threadIdx.x + 1;\n"
         "  numRows = state->resultRowPointers[shared->streamIdx][0];\n"
         "  if (rowIdx <= numRows) {\n"
         "  auto state = reinterpret_cast<DeviceAggregation*>(shared->states["
      << stateOrdinal
      << "]);\n"
         "    row = reinterpret_cast<HashRow*>(\n"
         "      state->resultRowPointers[shared->streamIdx][rowIdx]);\n";
  // Copy keys and accumulators to output.
  for (auto i = 0; i < read.probe->keys.size(); ++i) {
    out << extractColumn(
        "row",
        fmt::format("key{}", i),
        i,
        state.ordinal(*read.keys[i]),
        *read.keys[i]);
  }
  out << readAggRow(state, read);
  out << "  if (threadIdx.x == 0) {\n"
      << "    shared->numRows = rowIdx + kBlockSize <= numRows \n"
      << "   ? kBlockSize \n"
      << "    : numRows - blockIdx.x * kBlockSize;\n"
      << "  }\n"
      << "    }\n";
}

std::string streamToString(std::stringstream* s) {
  return s->str();
}

} // namespace facebook::velox::wave
