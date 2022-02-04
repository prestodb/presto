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
#include <string>

namespace facebook::velox::functions {
// TODO: move functions that are not shared with spark to prestosql and avoid
// the indirection here.
extern void registerArithmeticFunctions();
extern void registerCheckedArithmeticFunctions();
extern void registerComparisonFunctions();
extern void registerArrayFunctions();
extern void registerMapFunctions();
extern void registerJsonFunctions();
extern void registerHyperLogFunctions();
extern void registerGeneralFunctions();
extern void registerDateTimeFunctions();
extern void registerURLFunctions();
extern void registerStringFunctions();
extern void registerBitwiseFunctions();
extern void registerMapAllowingDuplicates(const std::string& name);

namespace prestosql {
void registerArithmeticFunctions() {
  functions::registerArithmeticFunctions();
}

void registerCheckedArithmeticFunctions() {
  functions::registerCheckedArithmeticFunctions();
}

void registerComparisonFunctions() {
  functions::registerComparisonFunctions();
}

void registerArrayFunctions() {
  functions::registerArrayFunctions();
}

void registerMapFunctions() {
  functions::registerMapFunctions();
}

void registerJsonFunctions() {
  functions::registerJsonFunctions();
}

void registerHyperLogFunctions() {
  functions::registerHyperLogFunctions();
}

void registerGeneralFunctions() {
  functions::registerGeneralFunctions();
}

void registerDateTimeFunctions() {
  functions::registerDateTimeFunctions();
}

void registerURLFunctions() {
  functions::registerURLFunctions();
}

void registerStringFunctions() {
  functions::registerStringFunctions();
}

void registerBitwiseFunctions() {
  functions::registerBitwiseFunctions();
}

void registerAllScalarFunctions() {
  registerArithmeticFunctions();
  registerCheckedArithmeticFunctions();
  registerComparisonFunctions();
  registerMapFunctions();
  registerArrayFunctions();
  registerJsonFunctions();
  registerHyperLogFunctions();
  registerGeneralFunctions();
  registerDateTimeFunctions();
  registerURLFunctions();
  registerStringFunctions();
  registerBitwiseFunctions();
}

void registerMapAllowingDuplicates(const std::string& name) {
  functions::registerMapAllowingDuplicates(name);
}
} // namespace prestosql

} // namespace facebook::velox::functions
