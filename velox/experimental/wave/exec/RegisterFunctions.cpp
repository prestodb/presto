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

void registerWaveFunctions() {
  registerBinaryNumeric(
      "plus", FunctionMetadata(), "$R$ plus($1$ x, $2$ y) { return x + y; }");
  registerBinaryNumeric(
      "lt", FunctionMetadata(), "bool lt($1$ x, $2$ y) { return x < y; }");
}

} // namespace facebook::velox::wave
