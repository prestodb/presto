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

#include "velox/connectors/fuzzer/FuzzerConnector.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

namespace facebook::velox::connector::fuzzer::test {

class FuzzerConnectorTestBase : public exec::test::OperatorTestBase {
 public:
  const std::string kFuzzerConnectorId = "test-fuzzer";

  void SetUp() override {
    OperatorTestBase::SetUp();
    connector::fuzzer::FuzzerConnectorFactory factory;
    auto fuzzerConnector = factory.newConnector(kFuzzerConnectorId, nullptr);
    connector::registerConnector(fuzzerConnector);
  }

  void TearDown() override {
    connector::unregisterConnector(kFuzzerConnectorId);
    OperatorTestBase::TearDown();
  }

  exec::Split makeFuzzerSplit(size_t numRows) const {
    return exec::Split(
        std::make_shared<FuzzerConnectorSplit>(kFuzzerConnectorId, numRows));
  }

  std::vector<exec::Split> makeFuzzerSplits(
      size_t rowsPerSplit,
      size_t numSplits) const {
    std::vector<exec::Split> splits;
    splits.reserve(numSplits);

    for (size_t i = 0; i < numSplits; ++i) {
      splits.emplace_back(makeFuzzerSplit(rowsPerSplit));
    }
    return splits;
  }

  std::shared_ptr<FuzzerTableHandle> makeFuzzerTableHandle(
      size_t fuzzerSeed = 0) const {
    return std::make_shared<FuzzerTableHandle>(
        kFuzzerConnectorId, fuzzerOptions_, fuzzerSeed);
  }

 private:
  VectorFuzzer::Options fuzzerOptions_;
};

} // namespace facebook::velox::connector::fuzzer::test
