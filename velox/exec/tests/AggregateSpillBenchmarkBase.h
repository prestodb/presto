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

#include "velox/exec/tests/SpillerBenchmarkBase.h"

namespace facebook::velox::exec::test {
class AggregateSpillBenchmarkBase : public SpillerBenchmarkBase {
 public:
  AggregateSpillBenchmarkBase() = default;

  /// Sets up the test.
  void setUp() override;

  /// Runs the test.
  void run() override;

 private:
  std::unique_ptr<RowContainer> makeRowContainer(
      const std::vector<TypePtr>& keyTypes,
      const std::vector<TypePtr>& dependentTypes) const;
  std::unique_ptr<RowContainer> setupSpillContainer(
      const RowTypePtr& rowType,
      uint32_t numKeys) const;
  void writeSpillData();

  std::unique_ptr<RowContainer> rowContainer_;
};
} // namespace facebook::velox::exec::test
