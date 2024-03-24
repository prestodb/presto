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
  explicit AggregateSpillBenchmarkBase(Spiller::Type spillerType)
      : spillerType_(spillerType){};

  /// Sets up the test.
  void setUp() override;

  /// Runs the test.
  void run() override;

  void printStats() const override;

 private:
  void writeSpillData();
  std::unique_ptr<Spiller> makeSpiller();

  const Spiller::Type spillerType_;
  std::unique_ptr<RowContainer> rowContainer_;
  std::shared_ptr<velox::memory::MemoryPool> spillerPool_;
};
} // namespace facebook::velox::exec::test
