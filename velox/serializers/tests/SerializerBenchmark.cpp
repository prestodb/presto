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
#include <folly/init/Init.h>

#include <vector>
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/memory/ByteStream.h"
#include "velox/common/time/Timer.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::test;

class SerializerBenchmark : public VectorTestBase {
 public:
  struct SerializeCase {
    int32_t nullPct;
    int32_t numSelected;
    int32_t bits;
    int32_t vectorSize;
    uint64_t irTime{0};
    uint64_t rrTime{0};

    std::string toString() {
      return fmt::format(
          "{} of {} {} bit {}%null: {} ir / {} rr",
          numSelected,
          vectorSize,
          bits,
          nullPct,
          irTime,
          rrTime);
    }
  };

  void setup() {
    serde_ = std::make_unique<serializer::presto::PrestoVectorSerde>();
  }
  void timeFlat() {
    // Serialize different fractions of a 10K vector of int32_t and int64_t with
    // IndexRange and row range variants with and without nulls.
    constexpr int32_t kPad = 8;
    std::vector<int32_t> numSelectedValues = {3, 30, 300, 10000};
    std::vector<std::vector<IndexRange>> indexRanges;
    std::vector<std::vector<vector_size_t>> rowSets;
    std::vector<int32_t> nullPctValues = {0, 1, 10, 90};
    std::vector<int32_t> bitsValues = {32, 64};
    const int32_t vectorSize = 10000;
    for (auto numSelected : numSelectedValues) {
      std::vector<IndexRange> ir;
      std::vector<vector_size_t> rr;
      int32_t step = vectorSize / numSelected;
      for (auto r = 0; r < vectorSize; r += step) {
        ir.push_back(IndexRange{r, 1});
        rr.push_back(r);
      }
      std::cout << rr.size();
      indexRanges.push_back(std::move(ir));
      rr.resize(rr.size() + kPad, 999999999);
      rowSets.push_back(std::move(rr));
    }
    VectorMaker vm(pool_.get());
    std::vector<VectorPtr> v32s;
    std::vector<VectorPtr> v64s;
    for (auto nullPct : nullPctValues) {
      auto v32 = vm.flatVector<int32_t>(
          vectorSize,
          [&](auto row) { return row; },
          [&](auto row) { return nullPct == 0 ? false : row % 100 < nullPct; });
      auto v64 = vm.flatVector<int64_t>(
          vectorSize,
          [&](auto row) { return row; },
          [&](auto row) { return nullPct == 0 ? false : row % 100 < nullPct; });
      v32s.push_back(std::move(v32));
      v64s.push_back(std::move(v64));
    }
    std::vector<SerializeCase> cases;
    Scratch scratch;

    auto runCase = [&](int32_t nullIdx, int32_t selIdx, int32_t bits) {
      SerializeCase item;
      item.vectorSize = vectorSize;
      item.nullPct = nullPctValues[nullIdx];
      item.numSelected = numSelectedValues[selIdx];
      item.bits = bits;
      int32_t numRepeat = 100 * vectorSize / indexRanges[selIdx].size();

      VectorPtr vector = bits == 32 ? v32s[nullIdx] : v64s[nullIdx];
      auto rowType = ROW({vector->type()});
      auto rowVector = vm.rowVector({vector});
      {
        MicrosecondTimer t(&item.irTime);
        auto group = std::make_unique<VectorStreamGroup>(pool_.get());
        group->createStreamTree(rowType, rowSets[selIdx].size() - kPad);
        for (auto repeat = 0; repeat < numRepeat; ++repeat) {
          group->append(
              rowVector,
              folly::Range(
                  indexRanges[selIdx].data(), indexRanges[selIdx].size()),
              scratch);
        }
      }

      {
        MicrosecondTimer t(&item.rrTime);
        auto group = std::make_unique<VectorStreamGroup>(pool_.get());
        group->createStreamTree(rowType, rowSets[selIdx].size());

        for (auto repeat = 0; repeat < numRepeat; ++repeat) {
          group->append(
              rowVector,
              folly::Range(
                  rowSets[selIdx].data(), rowSets[selIdx].size() - kPad),
              scratch);
        }
      }
      return item;
    };

    for (auto bits : bitsValues) {
      for (auto nullIdx = 0; nullIdx < nullPctValues.size(); ++nullIdx) {
        for (auto selIdx = 0; selIdx < numSelectedValues.size(); ++selIdx) {
          int32_t numRepeat = 10 / numSelectedValues[selIdx];
          cases.push_back(runCase(nullIdx, selIdx, bits));
        }
      }
    }
    for (auto& item : cases) {
      std::cout << item.toString() << std::endl;
    }
  }

  std::unique_ptr<serializer::presto::PrestoVectorSerde> serde_;
};

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv};
  serializer::presto::PrestoVectorSerde::registerVectorSerde();
  SerializerBenchmark bm;
  bm.setup();
  bm.timeFlat();
}
