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

void makeKeyMembers(
    const std::vector<AbstractOperand*>& keys,
    const std::string& prefix,
    std::stringstream& out) {
  for (auto i = 0; i < keys.size(); ++i) {
    auto* key = keys[i];
    VELOX_CHECK_NOT_NULL(key);
    out << cudaTypeName(*key->type) << " " << prefix << i << ";\n";
  }
}

void makeProbeKeyMembers(
    const std::vector<AbstractOperand*>& keys,
    std::stringstream& out) {
  for (auto n = 0; n < keys.size(); n += 32) {
    out << fmt::format("  uint32_t nulls{};\n", n / 32);
  }
  for (auto i = 0; i < keys.size(); ++i) {
    auto* key = keys[i];
    out << cudaTypeName(*key->type) << " key" << i << ";\n";
  }
}

void makeHash(
    CompileState& state,
    const std::vector<AbstractOperand*>& keys,
    bool nullableKeys,
    std::string nullCode,
    int32_t id) {
  auto& out = state.generated();
  std::string idStr;
  if (id != -1) {
    idStr = fmt::format("{}", id);
  }
  out << "  hash" << idStr << " = 1;\n";
  for (auto i = 0; i < keys.size(); ++i) {
    auto* op = keys[i];
    state.ensureOperand(op);
    std::string stmt;
    if (!nullableKeys && !op->notNull) {
      stmt = fmt::format(
          "  if ({}) {{ goto nullKey; }}\n"
          "   hash{} = hashMix(hash{}, hashValue({}));\n",
          state.isNull(op),
          id,
          id,
          state.operandValue(op));
    } else {
      if (!keys[i]->notNull) {
        stmt = fmt::format(
            "  if ({}) {{ $h$ = hashMix($h$, 13); }} else {{ $h$ = hashMix($h$, hashValue({})); }}\n",
            state.isNull(op),
            state.operandValue(op));
      } else {
        stmt = fmt::format(
            "  $h$ = hashMix($h$, hashValue({}));\n", state.operandValue(op));
      }
    }
    out << replaceAll(stmt, "$h$", fmt::format("hash{}", idStr));
  }
  if (!nullableKeys) {
    out << " goto hashDone;\n"
        << " nullKey: laneStatus = ErrorCode::kInactive;\n"
        << nullCode << "\n  hashDone: ;\n";
  }
}

void makeCompareLambda(
    CompileState& state,
    const std::vector<AbstractOperand*>& keys,
    bool nullableKeys,
    int32_t id) {
  auto& out = state.generated();
  out << "  [&](HashRow" << id << "* row) -> bool {\n";
  if (nullableKeys) {
    out << "   keyNulls = asDeviceAtomic<uint32_t>(&row->nulls0)->template load<MemoryOrder::kAcquire>();\n";
    VELOX_CHECK_LE(keys.size(), 32);
  }
  for (auto i = 0; i < keys.size(); ++i) {
    auto* op = keys[i];
    if (nullableKeys && !op->notNull) {
      out << fmt::format(
          "  if ({} != (0 == (keyNulls & (1U << {})))) return false;\n",
          state.isNull(op),
          i / 32,
          i & 31);
    }
    out << fmt::format(
        "  if ({} != row->key{}) return false;\n", state.operandValue(op), i);
  }
  out << "  return true;\n}\n";
}

std::string initRowNullFlags(
    CompileState& state,
    int32_t begin,
    int32_t end,
    const OpVector& keys) {
  std::stringstream inits;
  for (auto i = begin; i < end; ++i) {
    inits << fmt::format(
        "({} ? 0 : {}U) {}",
        state.isNull(keys[i]),
        1U << i,
        (i < end - 1 ? " | " : ""));
  }
  return inits.str();
}

void makeInitGroupRow(
    CompileState& state,
    const OpVector& keys,
    const std::vector<const AggregateUpdate*>& aggregates,
    int32_t id) {
  auto& out = state.generated();
  out << "  [&](HashRow" << id << "* row) {\n";
  int32_t numNullFlags = aggregates.size() + keys.size();
  for (auto i = 0; i < keys.size(); ++i) {
    auto* op = keys[i];
    out << fmt::format(
        "   if (!{}) {{ row->key{} = {};}}\n",
        state.isNull(op),
        i,
        state.operandValue(op));
  }

  for (auto i = 0; i < aggregates.size(); ++i) {
    auto* update = aggregates[i];
    out << update->generator->generateInit(state, *update);
  }

  VELOX_CHECK_LE(keys.size(), 32);
  for (auto i = 32; i < numNullFlags; i += 32) {
    out << fmt::format("   row->nulls{} = 0;\n", i / 32);
  }
  out << fmt::format(
      "  asDeviceAtomic<uint32_t>(&row->nulls0)->template store<MemoryOrder::kRelease>(keyNulls = {});\n",
      initRowNullFlags(state, 0, keys.size(), keys));
  out << "}\n";
}

void makeRowHash(
    CompileState& state,
    const std::vector<AbstractOperand*>& keys,
    bool nullableKeys,
    int32_t id) {
  auto& out = state.inlines();
  out << "  uint64_t __device__ hashRow(HashRow" << id << "* row) {\n"
      << "  uint64_t hash = 1;\n";
  for (auto i = 0; i < keys.size(); ++i) {
    if (nullableKeys) {
      out << fmt::format(
          "    if (0 == (row->nulls{} & (1U << {}))) {{ hash = hashMix(hash, 13); }} else {{",
          i / 32,
          i & 32);
    }
    out << fmt::format("    hash = hashMix(hash, hashValue(row->key{}));\n", i);
    if (nullableKeys) {
      out << "  }\n";
    }
  }
  out << "  return hash;\n}\n";
}

std::string extractColumn(
    const std::string& row,
    const std::string& field,
    int32_t nthNull,
    int32_t ordinal,
    const AbstractOperand& result) {
  std::stringstream out;
  switch (result.type->kind()) {
    case TypeKind::BIGINT:
      out << fmt::format(
          "    flatResult<{}>(operands, {}, blockBase) = {}->{};\n",
          cudaTypeName(*result.type),
          ordinal,
          row,
          field);
      break;
    default:
      VELOX_NYI();
  }
  if (!result.notNull) {
    out << fmt::format(
        "  setNull(operands, {}, blockBase, ({}->nulls{} & (1U << {})) == 0);\n",
        ordinal,
        row,
        nthNull / 32,
        nthNull & 31);
  }
  return out.str();
}

} // namespace facebook::velox::wave
