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

#include "velox/exec/tests/utils/FunctionUtils.h"
#include "velox/functions/prestosql/tests/FunctionBaseTest.h"
#include "velox/functions/sparksql/Register.h"

namespace facebook::velox::functions::sparksql::test {

using facebook::velox::functions::test::FunctionBaseTest;

class SparkFunctionBaseTest : public FunctionBaseTest {
 protected:
  // Ensure Spark functions are registered; don't register the "common"
  // (CoreSQL) functions.
  static void SetUpTestCase() {
    exec::test::registerTypeResolver();
    sparksql::registerFunctions("");
  }
};

} // namespace facebook::velox::functions::sparksql::test
