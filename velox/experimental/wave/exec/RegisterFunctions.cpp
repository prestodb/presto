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
#include "velox/experimental/wave/exec/WaveRegistry.h"

namespace facebook::velox::wave {
namespace {
bool registerBinaryNumeric(
    const std::string& name,
    FunctionMetadata metadata,
    const std::string& text) {
  static std::vector<TypePtr> types = {
      TINYINT(), SMALLINT(), INTEGER(), BIGINT(), REAL(), DOUBLE()};
  for (auto& type : types) {
    std::vector<TypePtr> args{type, type};
    FunctionKey key(name, args);
    waveRegistry().registerFunction(key, metadata, "", text);
  }
  return true;
}

} // namespace

#define FULL_BINARY_ARGS                                         \
  "(WaveShared* shared, ErrorCode& laneStatus, bool insideTry, " \
  "int32_t grid, int32_t block, $1$ x, $2$ y)"

#define CHECK_DIV0 \
  "  if (y == 0) { setError(shared, laneStatus, insideTry, 1); return 0; }\n"

const char* divideText =
    "$R$ divide" FULL_BINARY_ARGS "{\n" CHECK_DIV0 " return x / y; }";

const char* modText =
    "$R$ mod" FULL_BINARY_ARGS "{\n" CHECK_DIV0 " return x % y; }";

void registerWaveFunctions() {
  registerBinaryNumeric(
      "plus", FunctionMetadata(), "$R$ plus($1$ x, $2$ y) { return x + y; }");
  registerBinaryNumeric(
      "minus", FunctionMetadata(), "$R$ minus($1$ x, $2$ y) { return x - y; }");
  registerBinaryNumeric(
      "multiply",
      FunctionMetadata(),
      "$R$ multiply($1$ x, $2$ y) { return x * y; }");
  registerBinaryNumeric("divide", FunctionMetadata(true, true), divideText);
  registerBinaryNumeric("mod", FunctionMetadata(true, true), modText);

  registerBinaryNumeric(
      "lt", FunctionMetadata(), "bool lt($1$ x, $2$ y) { return x < y; }");
  registerBinaryNumeric(
      "lte", FunctionMetadata(), "bool lte($1$ x, $2$ y) { return x <= y; }");
  registerBinaryNumeric(
      "eq", FunctionMetadata(), "bool eq($1$ x, $2$ y) { return x == y; }");
  registerBinaryNumeric(
      "neq", FunctionMetadata(), "bool neq($1$ x, $2$ y) { return x != y; }");
  registerBinaryNumeric(
      "gt", FunctionMetadata(), "bool gt($1$ x, $2$ y) { return x > y; }");
  registerBinaryNumeric(
      "gte", FunctionMetadata(), "bool gte($1$ x, $2$ y) { return x >= y; }");

  waveRegistry().registerMessage(1, "Divide by 0");
}

} // namespace facebook::velox::wave
