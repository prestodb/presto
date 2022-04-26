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

#include "velox/exec/tests/utils/OperatorTestBase.h"

namespace facebook::velox::aggregate::test {

class AggregationTestBase : public exec::test::OperatorTestBase {
 protected:
  std::vector<RowVectorPtr>
  makeVectors(const RowTypePtr& rowType, vector_size_t size, int numVectors);
};

} // namespace facebook::velox::aggregate::test
