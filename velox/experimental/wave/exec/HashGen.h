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

#include "velox/experimental/wave/exec/ToWave.h"

/// Functions for generating device side hash tables and functions on them.
namespace facebook::velox::wave {

void makeKeyMembers(
    const std::vector<AbstractOperand*>& keys,
    const std::string& prefix,
    std::stringstream& out);

/// Emits code for loading hash lookup operands and computing a hash
/// number. 'nullableKeys' is true for group by and false for join. If
/// 'nullableKeys' is true, 'anyNullCode' is emitted for the case of
/// at least one null in the keys.
void makeHash(
    CompileState& state,
    const std::vector<AbstractOperand*>& keys,
    bool nullableKeys,
    std::string anyNullCode = "",
    int32_t id = -1);

/// Emits a lambda for comparing hash table row with probe keys. 'nullableKeys'
/// is true for group by. The signature is [&](HashRow* row) -> bool.
void makeCompareLambda(
    CompileState& state,
    const std::vector<AbstractOperand*>& keys,
    bool nullableKeys,
    int32_t id);

/// Emits a lambda to initialize a new group by row or keys of a hash join build
/// row. 'nullableKeys' is true for group by. The signature is [&](GroupRow*
/// row).
void makeInitGroupRow(
    CompileState& state,
    const OpVector& keys,
    const std::vector<const AggregateUpdate*>& aggregates,
    int32_t id);

void makeRowHash(
    CompileState& state,
    const std::vector<AbstractOperand*>& keys,
    bool nullableKeys,
    int32_t id);

std::string extractColumn(
    const std::string& row,
    const std::string& field,
    int32_t nthNull,
    int32_t ordinal,
    const AbstractOperand& result);

/// Makes an expression to init null flags for a set of Operands, one
/// bit per Operand. 'begin' and 'end' are indices into 'keys'. The
/// range should be <= 32 elements of 'keys'.
std::string initRowNullFlags(
    CompileState& state,
    int32_t begin,
    int32_t end,
    const OpVector& keys);

} // namespace facebook::velox::wave
