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
#include <cuda_runtime.h> // @manual
#include "velox/common/base/tests/GTestUtils.h"

#include "velox/exec/ExchangeSource.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/HashJoinTestBase.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/LocalExchangeSource.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/experimental/wave/exec/ToWave.h"
#include "velox/experimental/wave/exec/WaveHiveDataSource.h"
#include "velox/experimental/wave/exec/tests/utils/FileFormat.h"
#include "velox/experimental/wave/exec/tests/utils/WaveTestSplitReader.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

DECLARE_int32(wave_max_reader_batch_rows);
DECLARE_int32(max_streams_per_driver);
DECLARE_int32(wave_reader_rows_per_tb);

using namespace facebook::velox;
using namespace facebook::velox::core;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

class HashJoinTest : public virtual exec::test::HashJoinTestBase {
 protected:
  void SetUp() override {
    HashJoinTestBase::SetUp();
    if (int device; cudaGetDevice(&device) != cudaSuccess) {
      GTEST_SKIP() << "No CUDA detected, skipping all tests";
    }
    wave::registerWave();
    wave::WaveHiveDataSource::registerConnector();
    wave::test::WaveTestSplitReader::registerTestSplitReader();
    exec::ExchangeSource::factories().clear();
    exec::ExchangeSource::registerFactory(createLocalExchangeSource);
  }

  static void SetUpTestCase() {
    OperatorTestBase::SetUpTestCase();
  }

  void TearDown() override {
    wave::test::Table::dropAll();
    HiveConnectorTestBase::TearDown();
  }
};

TEST_F(HashJoinTest, twoKeys) {
  probeType_ = ROW({"t_k1", "t_k2", "t_data"}, {BIGINT(), BIGINT(), BIGINT()});
  buildType_ = ROW({"u_k1", "u_k2", "u_data"}, {BIGINT(), BIGINT(), BIGINT()});

  auto build = makeRowVector(
      {"u_k1", "u_k2", "u_data"},
      {makeFlatVector<int64_t>(1000, [&](auto r) { return r; }),
       makeFlatVector<int64_t>(1000, [&](auto r) { return r; }),
       makeFlatVector<int64_t>(1000, [&](auto r) { return r; })});
  auto probe = makeRowVector(
      {"t_k1", "t_k2", "t_data"},
      {makeFlatVector<int64_t>(1000, [&](auto r) { return r + 100; }),
       makeFlatVector<int64_t>(1000, [&](auto r) { return r + 100; }),
       makeFlatVector<int64_t>(1000, [&](auto r) { return r + 2; })});

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .numDrivers(1)
      .probeType(probeType_)
      .probeKeys({"t_k1", "t_k2"})
      .probeVectors({probe})

      .buildType(buildType_)
      .buildKeys({"u_k1", "u_k2"})
      .buildVectors({build})
      .injectSpill(false)
      .referenceQuery(
          "SELECT t_k1, t_k2, t_data, u_k1, u_k2, u_data FROM t, u WHERE t_k1 = u_k1 AND t_k2 = u_k2")
      .run();
}

TEST_F(HashJoinTest, manyHits) {
  probeType_ = ROW({"t_k1", "t_k2", "t_data"}, {BIGINT(), BIGINT(), BIGINT()});
  buildType_ = ROW({"u_k1", "u_k2", "u_data"}, {BIGINT(), BIGINT(), BIGINT()});

  int32_t numRepeats = 20;
  auto build = makeRowVector(
      {"u_k1", "u_k2", "u_data"},
      {makeFlatVector<int64_t>(
           15000, [&](auto r) { return (r / numRepeats) * 9; }),
       makeFlatVector<int64_t>(
           15000, [&](auto r) { return (r / numRepeats) * 9; }),
       makeFlatVector<int64_t>(15000, [&](auto r) { return r; })});
  auto probe = makeRowVector(
      {"t_k1", "t_k2", "t_data"},
      {makeFlatVector<int64_t>(1000, [&](auto r) { return r * 3; }),
       makeFlatVector<int64_t>(1000, [&](auto r) { return r * 3; }),
       makeFlatVector<int64_t>(1000, [&](auto r) { return r; })});

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .numDrivers(1)
      .probeType(probeType_)
      .probeKeys({"t_k1", "t_k2"})
      .probeVectors({probe})

      .buildType(buildType_)
      .buildKeys({"u_k1", "u_k2"})
      .buildVectors({build})
      .injectSpill(false)
      .referenceQuery(
          "SELECT t_k1, t_k2, t_data, u_k1, u_k2, u_data FROM t, u WHERE t_k1 = u_k1 AND t_k2 = u_k2")
      .run();
}

TEST_F(HashJoinTest, DISABLED_twoKeysLeft) {
  probeType_ = ROW({"t_k1", "t_k2", "t_data"}, {BIGINT(), BIGINT(), BIGINT()});
  buildType_ = ROW({"u_k1", "u_k2", "u_data"}, {BIGINT(), BIGINT(), BIGINT()});

  auto build = makeRowVector(
      {"u_k1", "u_k2", "u_data"},
      {makeFlatVector<int64_t>(1000, [&](auto r) { return r; }),
       makeFlatVector<int64_t>(1000, [&](auto r) { return r; }),
       makeFlatVector<int64_t>(1000, [&](auto r) { return r; })});
  auto probe = makeRowVector(
      {"t_k1", "t_k2", "t_data"},
      {makeFlatVector<int64_t>(1000, [&](auto r) { return r + 100; }),
       makeFlatVector<int64_t>(1000, [&](auto r) { return r + 100; }),
       makeFlatVector<int64_t>(1000, [&](auto r) { return r + 2; })});

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .numDrivers(1)
      .probeType(probeType_)
      .probeKeys({"t_k1", "t_k2"})
      .probeVectors({probe})
      .joinType(core::JoinType::kLeft)
      .buildType(buildType_)
      .buildKeys({"u_k1", "u_k2"})
      .buildVectors({build})
      .injectSpill(false)
      .referenceQuery(
          "SELECT t_k1, t_k2, t_data, u_k1, u_k2, u_data FROM t LEFT JOIN u ON t_k1 = u_k1 AND t_k2 = u_k2")
      .run();
}

TEST_F(HashJoinTest, DISABLED_manyHitsLeft) {
  probeType_ = ROW({"t_k1", "t_k2", "t_data"}, {BIGINT(), BIGINT(), BIGINT()});
  buildType_ = ROW({"u_k1", "u_k2", "u_data"}, {BIGINT(), BIGINT(), BIGINT()});

  int32_t numRepeats = 20;
  auto build = makeRowVector(
      {"u_k1", "u_k2", "u_data"},
      {makeFlatVector<int64_t>(
           15000, [&](auto r) { return (r / numRepeats) * 9; }),
       makeFlatVector<int64_t>(
           15000, [&](auto r) { return (r / numRepeats) * 9; }),
       makeFlatVector<int64_t>(15000, [&](auto r) { return r; })});
  auto probe = makeRowVector(
      {"t_k1", "t_k2", "t_data"},
      {makeFlatVector<int64_t>(1000, [&](auto r) { return r * 3; }),
       makeFlatVector<int64_t>(1000, [&](auto r) { return r * 3; }),
       makeFlatVector<int64_t>(1000, [&](auto r) { return r; })});

  HashJoinBuilder(*pool_, duckDbQueryRunner_, driverExecutor_.get())
      .numDrivers(1)
      .probeType(probeType_)
      .probeKeys({"t_k1", "t_k2"})
      .probeVectors({probe})
      .joinType(core::JoinType::kLeft)
      .buildType(buildType_)
      .buildKeys({"u_k1", "u_k2"})
      .buildVectors({build})
      .injectSpill(false)
      .referenceQuery(
          "SELECT t_k1, t_k2, t_data, u_k1, u_k2, u_data FROM t LEFT JOIN u ON t_k1 = u_k1 AND t_k2 = u_k2")
      .run();
}
