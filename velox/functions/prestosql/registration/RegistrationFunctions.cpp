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
#include "velox/functions/prestosql/IPAddressFunctions.h"
#include "velox/functions/prestosql/UuidFunctions.h"

namespace facebook::velox::functions {

extern void registerMathematicalFunctions(const std::string& prefix);
extern void registerMathematicalOperators(const std::string& prefix);
extern void registerProbabilityTrigonometryFunctions(const std::string& prefix);
extern void registerArrayFunctions(const std::string& prefix);
extern void registerBitwiseFunctions(const std::string& prefix);
extern void registerCheckedArithmeticFunctions(const std::string& prefix);
extern void registerComparisonFunctions(const std::string& prefix);
extern void registerDateTimeFunctions(const std::string& prefix);
extern void registerGeneralFunctions(const std::string& prefix);
extern void registerHyperLogFunctions(const std::string& prefix);
extern void registerTDigestFunctions(const std::string& prefix);
extern void registerQDigestFunctions(const std::string& prefix);
extern void registerSfmSketchFunctions(const std::string& prefix);
extern void registerEnumFunctions(const std::string& prefix);
extern void registerIntegerFunctions(const std::string& prefix);
extern void registerFloatingPointFunctions(const std::string& prefix);
extern void registerJsonFunctions(const std::string& prefix);
extern void registerMapFunctions(const std::string& prefix);
extern void registerStringFunctions(const std::string& prefix);
extern void registerBinaryFunctions(const std::string& prefix);
extern void registerURLFunctions(const std::string& prefix);
extern void registerDataSizeFunctions(const std::string& prefix);
extern void registerMapAllowingDuplicates(
    const std::string& name,
    const std::string& prefix);
extern void registerBingTileFunctions(const std::string& prefix);
#ifdef VELOX_ENABLE_GEO
extern void registerGeometryFunctions(const std::string& prefix);
#endif
extern void registerInternalArrayFunctions();

namespace prestosql {
void registerArithmeticFunctions(const std::string& prefix) {
  functions::registerMathematicalOperators(prefix);
  functions::registerMathematicalFunctions(prefix);
  functions::registerProbabilityTrigonometryFunctions(prefix);
}

void registerCheckedArithmeticFunctions(const std::string& prefix) {
  functions::registerCheckedArithmeticFunctions(prefix);
}

void registerComparisonFunctions(const std::string& prefix) {
  functions::registerComparisonFunctions(prefix);
}

void registerArrayFunctions(const std::string& prefix) {
  functions::registerArrayFunctions(prefix);
}

void registerMapFunctions(const std::string& prefix) {
  functions::registerMapFunctions(prefix);
}

void registerJsonFunctions(const std::string& prefix) {
  functions::registerJsonFunctions(prefix);
}

void registerHyperLogFunctions(const std::string& prefix) {
  functions::registerHyperLogFunctions(prefix);
}

void registerTDigestFunctions(const std::string& prefix) {
  functions::registerTDigestFunctions(prefix);
}

void registerQDigestFunctions(const std::string& prefix) {
  functions::registerQDigestFunctions(prefix);
}

void registerSfmSketchFunctions(const std::string& prefix) {
  functions::registerSfmSketchFunctions(prefix);
}

void registerEnumFunctions(const std::string& prefix) {
  functions::registerEnumFunctions(prefix);
}

void registerIntegerFunctions(const std::string& prefix) {
  functions::registerIntegerFunctions(prefix);
}

void registerFloatingPointFunctions(const std::string& prefix) {
  functions::registerFloatingPointFunctions(prefix);
}

void registerBingTileFunctions(const std::string& prefix) {
  functions::registerBingTileFunctions(prefix);
}

#ifdef VELOX_ENABLE_GEO
void registerGeometryFunctions(const std::string& prefix) {
  functions::registerGeometryFunctions(prefix);
}
#endif

void registerGeneralFunctions(const std::string& prefix) {
  functions::registerGeneralFunctions(prefix);
}

void registerDateTimeFunctions(const std::string& prefix) {
  functions::registerDateTimeFunctions(prefix);
}

void registerURLFunctions(const std::string& prefix) {
  functions::registerURLFunctions(prefix);
}

void registerStringFunctions(const std::string& prefix) {
  functions::registerStringFunctions(prefix);
}

void registerBinaryFunctions(const std::string& prefix) {
  functions::registerBinaryFunctions(prefix);
}

void registerBitwiseFunctions(const std::string& prefix) {
  functions::registerBitwiseFunctions(prefix);
}

void registerAllScalarFunctions(const std::string& prefix) {
  registerArithmeticFunctions(prefix);
  registerCheckedArithmeticFunctions(prefix);
  registerComparisonFunctions(prefix);
  registerMapFunctions(prefix);
  registerArrayFunctions(prefix);
  registerJsonFunctions(prefix);
  registerHyperLogFunctions(prefix);
  registerTDigestFunctions(prefix);
  registerQDigestFunctions(prefix);
  registerSfmSketchFunctions(prefix);
  registerEnumFunctions(prefix);
  registerIntegerFunctions(prefix);
  registerFloatingPointFunctions(prefix);
  registerBingTileFunctions(prefix);
#ifdef VELOX_ENABLE_GEO
  registerGeometryFunctions(prefix);
#endif
  registerGeneralFunctions(prefix);
  registerDateTimeFunctions(prefix);
  registerURLFunctions(prefix);
  registerStringFunctions(prefix);
  registerBinaryFunctions(prefix);
  registerBitwiseFunctions(prefix);
  registerUuidFunctions(prefix);
  registerIPAddressFunctions(prefix);
  registerDataSizeFunctions(prefix);
}

void registerMapAllowingDuplicates(
    const std::string& name,
    const std::string& prefix) {
  functions::registerMapAllowingDuplicates(name, prefix);
}

void registerInternalFunctions() {
  functions::registerInternalArrayFunctions();
}
} // namespace prestosql

} // namespace facebook::velox::functions
