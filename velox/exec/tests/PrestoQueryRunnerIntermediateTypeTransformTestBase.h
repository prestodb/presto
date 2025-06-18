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

#include <gtest/gtest.h>

#include "velox/exec/fuzzer/PrestoQueryRunnerIntermediateTypeTransforms.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

namespace facebook::velox::exec::test {

class PrestoQueryRunnerIntermediateTypeTransformTestBase
    : public functions::test::FunctionBaseTest {
 protected:
  void test(const VectorPtr& vector);

  void testDictionary(const VectorPtr& base);

  void testConstant(const VectorPtr& base);

  void test(const TypePtr& type);

  void testArray(const TypePtr& type);

  void testMap(const TypePtr& type);

  void testRow(const TypePtr& type);

  void testArray(const VectorPtr& vector);

  void testMap(const VectorPtr& keys, const VectorPtr& values);

  void testRow(
      std::vector<VectorPtr>&& vectors,
      std::vector<std::string> names);

 private:
  const int32_t kVectorSize = 100;
  VectorFuzzer fuzzer_{VectorFuzzer::Options{}, pool_.get(), 123};
};

} // namespace facebook::velox::exec::test
